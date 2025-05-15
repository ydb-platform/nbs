## Main Part

The code style is based on https://go.dev/doc/effective_go and https://github.com/golang/go/wiki/CodeReviewComments, but there are some exceptions.

## Exceptions that will not be refactored

- Fixed line length of 80 characters
- Mandatory empty newlines at the beginning of each function if the signature is multiline
- Nested errors are discouraged
- By default, nil value receivers are not supported for methods, and this is not explicitly checked
- Underscores are allowed in package names
- Comments for exported objects are not mandatory
- Comments do not need to start with the name of the object being commented on
- Newline after `}` (exception: defer is pressed against the block above without indentation, for example: https://github.com/ydb-platform/nbs/blob/9c6622e69132261561cf582d4c6d8ad4fdccd67f/cloud/disk_manager/pkg/admin/disks.go#L99)
- Newline before `)` in the case of multiline calls (example: https://github.com/ydb-platform/nbs/blob/9c6622e69132261561cf582d4c6d8ad4fdccd67f/cloud/disk_manager/pkg/admin/images.go#L123)
- Comments are formatted as valid sentences: starting with a capital letter and ending with a period
- Use ID in all identifiers (not Id) (except for protobufs)

## Exceptions that can be refactored throughout the code and removed later

- The constructor is not placed directly below the structure declaration
- Constructors are not mandatory for classes (not talking about data container classes like Point, but rather logical ones like Storage)
- Named function result arguments are allowed wherever desired, not only where they add readability to the API
- Writer functions to channels are not logically separated from Readers in some multithreaded code
- Mixing receiver arguments in struct methods is allowed
