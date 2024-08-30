# DM Tracing Design Doc

## What we want to achieve

Our goal is to set up tracing of requests in the Disk Manager service using [Open Telemetry](https://opentelemetry.io/). As a result, spans from Disk Manager should be visible on request traces along with the existing spans from other services.

For each request in DM and for each DM task, traces should allow tracking what stages the request/task went through, which services it interacted with and which requests is sended, and how long it took. If the request took too long, it should be clear from the trace where the delays occurred. If an error occurred, it should be clear where it occurred. **All this information should be easily readable for someone who is not an expert on Disk Manager.**

For example, let's say we receive a complaint about task slowdowns. There can be various reasons for this — all of them should be visible in the traces. Including:
- Long waiting for responses to requests to Blockstore/YDB/S3.
- The task was restarted many times due to recoverable errors from Blockstore/YDB/S3.
- The task waited for a long time for a child task to complete.
- A task waits a long time for the result of another task that is not a child (for example, retire base disk or acquire base disk wait for the base disk to be filled).
- No runner took on the execution of the task for a long time.
- The task loses information about its progress and therefore repeats the same actions many times.
- DM is not at all to blame; the brakes were in external services relative to DM/Blockstore.

## Span pipeline

### Sending spans to the backend

Spans need to be sent to the backend, where they will be stored and can be viewed. In OpenTelemetry, there is a tool called [Exporter](https://opentelemetry.io/docs/languages/go/exporters/) for this purpose. We will use the [grpc exporter](https://opentelemetry.io/docs/languages/go/exporters/#otlp-traces-over-grpc). This exporter can send spans to an arbitrary endpoint.

### Span context from incoming requests

We need to be able to associate our spans with those of the service sending requests to Disk Manager. This means that for each request, we must obtain the [trace context](https://opentelemetry.io/docs/concepts/context-propagation/), namely, the trace ID and span ID of the span that will be the parent for our span. The trace context in [W3C format](https://www.w3.org/TR/trace-context/) (traceparent and tracestate fields in grpc metadata) can be passed in each incoming request. We need to be able to parse these fields.

## Span structure

### Plan A: what was originally proposed

Some time ago, we discussed what span structure to create. Then we tentatively came to the following option (which does not seem to be fully achievable).

1. One span per each request in grpc facade. For example, a request to create a disk or a task metadata request. These spans are normally short (since even if DM schedules a task in response to a request, this task will be executed asynchronously). The parent span will be the span passed in the grpc metadata of the request.
2. Each task has a main span. All other spans related to this task are in the subtree under this span. The span begins when we go to schedule the task and ends when the task is completed or canceled.
3. Each logical step in the execution of a task should have its own span. Example: creating a disk. The steps are: going to Blockstore to create a disk, filling the disk (with a child data-plane task), going to Blockstore to create a checkpoint on the base disk, going to ydb to put the disk into ready status.
4. There should be spans corresponding to requests to other services (Blockstore, Filestore, DM, S3). Not all requests are considered worthy of a separate span. In some cases, it is better to create an event within the current span or not create anything. A separate span is only if the request is important (long and/or complex). An example: creating an nrd disk checkpoint is an important request.
5. Policy regarding repetitions of the same actions by other generations of tasks. We haven't decided exactly what to do, but there is an idea that we would like to avoid duplicate spans for the same action.
6. Policy when retrying the same request (for example, in Blockstore). We haven't decided here either.

This plan faces some difficulties that need to be considered.

### What are the difficulties?

**A.** You cannot start a span on one host and finish it on another. This makes points 2 and 3 impossible: it will not be possible to create a span that covers the entire life of the task. Moreover, it will not be possible to make a span covering the execution of a task by different runners (in other words, covering more than one generation of the task).

**B.** Idempotent requests. By design, Disk Manager can often make the same request many times. For example, a task may schedule the same child task several times or repeatedly go to Blockstore with a request to create a disk. It is not clear how to prevent duplicate requests from generating duplicate spans.

Firstly, in the DM code, we often don't even know if a request is repeated or not — we don't think about it and just rely on the idempotence of the request. For example, we can schedule the same task many times, and we don't check whether we scheduled it for the first time or repeatedly.

Secondly, there is a technical obstacle. Any started span must be eventually completed (this is a requirement of the go sdk: "Any Span that is created MUST also be ended" [link](https://pkg.go.dev/go.opentelemetry.io/otel/trace#Tracer)). That is, there is no way to say "we changed our mind, we no longer need this span". Perhaps sampling could help here — but tail sampling is not available out of the box in the go sdk (see more details in the paragraph "Sampling").

**C.** In general, we need to strike a balance between two things. On the one hand, there is the completeness of information contained in spans. On the other hand, there is the number of spans (so that there are not too many of them). I have a feeling that in Plan A this balance is shifted towards too little completeness of information.

I believe that there is no need to be afraid of a large (within reasonable limits) number of spans, since spans in a trace can be filtered. If in some trace a lot of similar spans create extra noise, or if someone does not want to look at spans from Disk Manager at all — they can be filtered out when viewing. Therefore, it is more important to invest in an attribute system that will make filtering convenient than to strictly save spans. You can also use sampling to protect yourself from an excessively large number of spans (see more details in the paragraph "Sampling").

At the same time, it is important that all useful information is always reflected. Almost any request can fail or end with an error. Therefore, we should strive not to be greedy with creating spans.

The described difficulties serve as motivation for the second phase of the plan.

### Plan B
1. One span per each request in grpc facade — everything is OK here, no changes.
2. Do not make a "main" span for the entire task execution. Instead, you can add the `task_id` attribute to the span, which will allow filtering the spans of this task. The parent span for this task will be the span within which the task was first scheduled. It may be a span of a request in the grpc facade or one of the spans within the execution of the parent task.

	A nice consequence of this approach: the "real" moment of scheduling a child task can be distinguished from all repeated ones — the "real" one was in the parent span.
3. We cannot make a single span for a logical step in the work of the task. But we need to be able to isolate these logical steps from the trace somehow. This means that we will have to represent a logical step as a subset of spans. To filter spans for a given logical step, you can add an appropriate attribute to each span (call this attribute something like "step_description").
4. Requests to other services.
	- Each control-plane request to Blockstore/Filestore client has its own span. There are no unimportant requests here. If it still turns out that there are too many spans due to such a policy, it will be possible to remove spans for certain types of requests depending on the importance/noisiness ratio.
	- data-plane requests to Blockstore, as well as requests to S3. Here I propose making spans with strict sampling. After all, sometimes such requests slow down, so some spans are needed. However, there are a lot of such requests, and obviously it won't work to make a span for each request.
	- Requests to ydb. Here, similarly to Blockstore, you can make a span for each request in the [ydb go sdk](https://github.com/ydb-platform/nbs/tree/main/vendor/github.com/ydb-platform/ydb-go-sdk/v3) (for example, a span for the begin transaction request). This can be done using the ydb go sdk. The ydb go sdk allows you to flexibly configure what spans to create and what not to — if necessary, these settings can be changed.

	See more details in the paragraph "Interaction with other services".
5. Repetitions of the same actions by other generations of tasks. There may be situations like this:
	- The older generation duplicates an action already performed by the younger generation.
	- The action is performed by a younger (outdated) generation. This can happen, for example, with a request to Blockstore.

	I propose making a span for a duplicate request anyway. Since a duplicate request may well turn out to be the most important due to some kind of bug — as it recently happened, for example, with creating a checkpoint with deleted data. Yes, normally there should not be such bugs. But traces are precisely important for those requests in which something went wrong. If we debug something later and there is no clue in the traces, it will be annoying. Moreover, usually before executing a request — when you need to decide whether to start a span — we don't even know if the request is a duplicate.

	Sampling will help to avoid too many spans due to this policy. We will also need to think through attributes that will help filter out duplicate requests.
6. Regarding the retry policy, I don't know for sure yet. You can try this option: one request — one span, regardless of the number of retries. But make an event for each retry, writing the reason for the retry in the event attributes.

	Why events instead of spans?

	Pros: all the desired information here is quite easily read from events — the number of retries, their reasons, the size of the intervals between retries. It seems that it is possible not to create spans here.

	Cons: the size of intervals is still more convenient and conceptually correct to look at spans. Also, we need to see if there is any information that may change from retry to retry, but which we would like to dig into the span rather than looking for it through events. For example, will client ID be such information? Or transaction ID?

	But there is also a difficulty in implementing retries. In the case of Blockstore/Filestore and ydb, the code responsible for retries usually lies in the corresponding sdk. Is it possible to ask this code to follow the desired retry policy? In the case of ydb, perhaps this is feasible — we will need to take a closer look at the settings offered by the ydb go sdk. In the case of Blockstore, apparently this will not work — after all, there is currently no tracing at the level of the Blockstore go sdk. If we decide to make tracing in the Blockstore go sdk, we will have to provide for the settings of the retry policy.

### Error handling

- If the request ended with an unrecoverable error, then you need to remember this error in the span attributes and set the Error status for the span ([doc](https://opentelemetry.io/docs/languages/go/instrumentation/#record-errors)).
- Other errors should also be remembered in the span attributes.
- If the request returned the S_ALREADY error or somehow else indicated that the request was a duplicate, you can add information to the span attributes that the request is a duplicate — so that it is convenient to filter such spans.

### Other details

- Regular tasks. Since we start them ourselves, the question arises where spans of such tasks will go and who will be the parent span. You can make a separate parent span for each launch of a regular task, to which all children will be tied. This span will be trivial: it will begin as soon as it ends. In truth, conceptually open telemetry [dictates](https://opentelemetry.io/docs/concepts/signals/traces/#span-events) to make an event in such cases, not a span. But here, due to the absence of a parent, you have to make a span. Thus, there will be one trace per each launch of the regular task.
- Another point about regular tasks: what if a regular task is very long or even eternal? Here, you will have to somehow break the trace into several, at some point start a new parent span. The details still need to be thought out.
- Links between spans. Sometimes a task depends on the results of another task that is not child for it. For example, retire base disk or acquire base disk wait for the base disk to be filled. In this case, it makes sense to use span links [general doc](https://opentelemetry.io/docs/concepts/signals/traces/#span-links), [implementation in go skd](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#Link) — this is a way to create a link between two spans (possibly from different traces). I'm not 100% sure that it will work, but it's worth trying.

Note: you need to make sure that one and the same span does not have too many incoming links (by default there is a limit on the number of links — 128, [doc](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#NewSpanLimits)).
- There are requests to ydb that have no parent span — these are requests to the task database in ydb, which are selected for execution. Such requests are made outside of any task or request in grpc facade, so these requests will not have a parent span. This results in one separate trace per each such request. You can think about whether it is possible to improve this situation somehow.

### Span attributes

- Span type (need to distinguish execution of a task within one generation, sending a request to another service, processing a request in grpc facade). If it's about a request to another service — then the name of the service the request goes to is also needed.
- Task ID
- Task type
- Generation of the task
- Our standard headers: x-operation-id, x-request-id, x-request-uid.
- Request name (for example: [CreateDisk](https://github.com/ydb-platform/nbs/blob/main/cloud/disk_manager/internal/pkg/services/disks/service.go#L309), [CreateSnapshot](https://github.com/ydb-platform/nbs/blob/main/cloud/disk_manager/internal/pkg/services/snapshots/service.go#L24))
- Information on whether the task is currently being executed or canceled. Maybe just use the host + runner ID.
- Error type
- Error text
- Information about what logical step of the task is being performed now (step_description). But here we still need to think more about whether such a thing is needed at all, and if so, which steps in which tasks should be highlighted.
- A flag indicating that the request is a duplicate — so that duplicates can be easily filtered out.

If it's about a request to Blockstore:
- Request type (for example: CreateDisk, CreateСheckpoint)
- FQDN of the host the request goes to
- Client ID
- Session ID (if any)

If it's about a request to ydb:
- Transaction ID.
- Path to the database
- Information about sql queries: list of tables involved and operations (select, upsert, ...) that we applied to them.

If the request is in S3:
- Bucket ID

TODO: anything else?

### Events

We create events in the following cases:

- Context cancellation. It should be clear from the name/attributes of the event why the context was cancelled.
- We received an error (whether retrialable or not).
- Any place where an interrupt execution error is thrown. It should be clear from the name / attributes of the event why we decided to interrupt the task execution.
- We start waiting for another task (as a special case for interrupt execution error).
- We waited for another task.
- Scheduling a child task.
- Going to retry the request.
- TODO: Is there anything else?

## Sampling

To protect ourselves from an excessively large number of spans, we can do sampling. It is supported in the go sdk: [doc](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#Sampler). The sampler can be flexibly configured — we need to understand what sampling policy will be best for us.

General [doc](https://opentelemetry.io/docs/languages/go/sampling/) about sampling in open telemetry.

Terminology: if we take a span (make a decision to ship it), we *sample* it. If we discard a span, we do not *sample* it.

### What we want to achieve with sampling

There are two parameters that are important to follow:
- The total flow of spans from the Disk Manager.
- The number of spans within one trace.

The total number of spans should already be within reasonable limits (at least at first) — it is unlikely to be more than thousands of spans per second. Indeed, most of the time, the number of inflight tasks for the entire DM + snapshot in the cluster is about several dozen; one task, if used reasonably, should not generate more than dozens of spans per second; there should not be many spans outside tasks either. When traces start working, we will need to monitor how many spans per second we get in practice, whether there are any high peaks.

It is more important to limit the number of spans in one trace, since we may have long tasks and they can have a large number of generations. As a guideline, I propose to ensure that there are no more than 1000 spans within one task. Again, we need to see how convenient the traces turn out to be in practice and adjust this guideline if necessary.

Note: sampling can be done at the level of individual spans or at the entire trace level. In the case of Disk Manager, sampling at the trace level is probably not necessary for us, so further discussion focuses on span-level sampling.

### Head sampling and Tail sampling

A natural idea for sampling is to sample a span if it is long or contains an error, and sample other spans with some probability. However, this requires making a sampling decision after the request ends, i.e., tail sampling is needed ([doc](https://opentelemetry.io/docs/concepts/sampling/#tail-sampling)), and there are difficulties with this.

There is head sampling and tail sampling. In the case of head, the decision on whether to sample a span is made at the beginning of the span, while in the case of tail, it is made at the end. However, it seems that tail sampling is not supported in the Go SDK: "The exact sampler you should use depends on your specific needs, but in general you should make a decision at the start of a trace" [link](https://opentelemetry.io/docs/languages/go/sampling/). After searching online, I found this product: [tail sampling processor](https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor). But it is not yet clear how applicable it is.

### Sampling policy (baseline)

As a baseline, I propose the following sampling policy.

When starting a span, we look at the task generation. If it is small, we definitely sample the span. If it is large, we sample the span with some probability. We can make several «steps», when there will be a lower probability for a larger generation.

A significant disadvantage of this approach is that if the task took a long time to generate and then ended, we risk losing spans related to the completion of the task, which may be interesting.

In addition, you can consider the following ideas:

- Implement sampling based on parent ([parent based](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace#ParentBased)). In simple terms, this means that a child span checks whether the parent span has been sampled, and if not, the child span is also not sampled. (For this purpose, each span under the hood has a `Sampled` flag. This flag is passed in the traceparent header, so the mechanism works for external spans as well.)

- Sample a span with some fixed probability. This approach may be suitable for data-plane requests to YDB and S3.

- Consider limiting the number of child spans for one parent. This solution will not work in case of a large number of task generations (since in this case, the parent could be open on another host). However, it can help if a task opens many spans within one generation. Also, in general, it is useful to prevent too many spans.

## Interaction with other services (Blockstore, Filestore, YDB, S3...)

### YDB

There are two «levels» of calls when accessing YDB.
1. Calls to the methods of [persistence.YDBClient](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L531). For example, [CreateOrAlterTable](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L547) or [Execute](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L592).
2. Calls directly to the [ydb go sdk](https://github.com/ydb-platform/nbs/tree/main/vendor/github.com/ydb-platform/ydb-go-sdk/v3). For example, [session.StreamExecuteScanQuery](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L395) or [tx.CommitTx](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L316).

Within one call of level 1, several calls of level 2 may occur (this is especially true for [persistence.YDBClient.Execute](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L592) — a complex series of operations can occur within the function passed to it).

Proposal:
- Crete a span per call of level 1.
- We leave calls of level 2 up to ydb go sdk — it works with open telemetry ([here](https://github.com/ydb-platform/ydb-go-sdk-otel) is their adapter for open telemetry). Also, ydb go sdk allows you to configure which operations we start a span for and which ones we don't ([here](https://github.com/ydb-platform/nbs/blob/main/vendor/github.com/ydb-platform/ydb-go-sdk/v3/trace/details.go#L31) is a list of available options). Now some of these options are even [set](https://github.com/ydb-platform/nbs/blob/main/cloud/tasks/persistence/ydb.go#L698) in our code — however, I don’t understand what these options affect, since we don’t use the ydb adapter for open telemetry yet.

### Blockstore

The case of Blockstore is generally similar to ydb. For example, the «level 1» method [CreateProxyOverlayDisk](https://github.com/ydb-platform/nbs/blob/main/cloud/disk_manager/internal/pkg/clients/nbs/client.go#L644) involves several calls to the [Blockstore go sdk](https://github.com/ydb-platform/nbs/tree/main/cloud/blockstore/public/sdk/go). However, unlike the ydb go sdk, tracing with open telemetry is not yet supported in the Blockstore go sdk. You can implement tracing following the example of ydb. Alternatively, for each type of request in the Blockstore go sdk, you can create a wrapper on the Disk Manager side that starts a span for this request.

## Implementation stages

The following key stages can be identified during implementation.

1. Hello world. Make sure that some first spans start shipping. For example, only spans of requests to the grpc facade (point 1 of the plan). To do this, you need to write trace initialization, add a trace exporter to the initialization, learn how to take the trace context from compute requests, and create spans themselves. This is done in this PR https://github.com/ydb-platform/nbs/pull/1696.
2. MVP. Add spans that allow you to get meaningful traces, which you can already try to debug problems with. To do this, add spans reflecting the work of tasks, as well as spans of requests to other services (points 2–4 of the plan). Add the most important attributes and events to spans. Also at this stage, it’s worth making some simple sampler that will help not to overwhelm the trace with many thousands of spans in extreme cases.
3. Adding details, improvements and enhancements. Here you can think about a smarter sampler, a more convenient and complete set of attributes for spans/events, span links, etc. At this stage, you will need to look at the experience with the previous version and consider what was uncomfortable and what is most important to add.

## Misc

### Important points during implementation

- The library for tracing in Go should be located in the [cloud/tasks](https://github.com/ydb-platform/nbs/tree/main/cloud/tasks) folder.
- Optionally, you can make this library independent of a specific tracing implementation (opentelemetry in our case). However, there is currently no particular need for this.
- The [interceptor](https://github.com/ydb-platform/nbs/blob/d307bc734c5494c7a686fd9155e352efa1c949ec/cloud/disk_manager/internal/pkg/facade/interceptor.go#L186) is responsible for spans of requests to the gprc facade. The [runners](https://github.com/ydb-platform/nbs/blob/d307bc734c5494c7a686fd9155e352efa1c949ec/cloud/tasks/runner.go) are responsible for task spans. Corresponding clients are responsible for spans of requests to other systems. Task code does not open spans, but it can modify the current span — add attributes and events.
- The runner that takes a task for execution needs to know the trace context for the span that will be the parent for this task's spans. For this purpose, the trace context is persistently stored in the [task metadata](https://github.com/ydb-platform/nbs/blob/d307bc734c5494c7a686fd9155e352efa1c949ec/cloud/tasks/storage/common.go#L226). This trace context is determined at the time of the first scheduling of the task and remains unchanged thereafter.
- It is probably necessary to add a shutdown timeout for tracing.
