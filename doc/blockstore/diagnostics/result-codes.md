# Result codes

> Header with codes declaration: cloud/storage/core/libs/common/error.h

## Success codes

    S_OK  // The request was completed successfully
    S_FALSE  // The request was not completed because the object does not exist; there is nothing to operate on
    S_ALREADY // The request was not completed because the object is already in the requested state

## Error codes

    E_INVALID_STATE // The object is in an inappropriate state to perform the request
    E_TIMEOUT  // The underlying layer failed to meet the deadline
    E_NOT_FOUND  // The object was not found
    E_UNAUTHORIZED  // Authorization error
    E_NOT_IMPLEMENTED  // The request is not implemented yet or should not be implemented at all
    E_ABORTED  // This request must not be retried; you should retry the higher-level request instead
    E_TRY_AGAIN  // The control-plane request cannot be executed at this time; please try again later. Unlike E_REJECTED, which can be retried immediately
    E_IO  // Input/output error. This is fatal, indicating it is impossible to read or write a block due to hardware failure
    E_CANCELLED  // The request execution was stopped at the initiative of the client (e.g., client disconnected)
    E_IO_SILENT  // A legacy code for input/output errors. Unlike E_IO, it does not increment the fatal error counter in monitoring
    E_RETRY_TIMEOUT  // The total time limit (24 hours) for executing the request has expired
    E_PRECONDITION_FAILED  // Preconditions within the object have been violated. This error is not retryable

> Header with flags declaration: cloud/storage/core/protos/error.proto

## Error Flags

    EF_SILENT  // A flag that makes the target error EErrorKind::ErrorSilent. Such an error does not increment the fatal error counter. Caution: using this flag with a retryable error makes it non-retryable
    EF_HW_PROBLEMS_DETECTED  // A flag indicating a hardware problem
