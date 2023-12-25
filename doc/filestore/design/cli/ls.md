# filestore-client ls

Analogue of Unix's `ls` tool with support of pagination and output in JSON format.

### Mandatory parameters:

> All following parameters are mutually exclusive

`--node <id>`

List content of specified `<id>` in a paged way with abilities to navigate to previous and next pages.
`<id>` is the inode's id.

`--path <path>`

List content of specified `<path>` in a paged way with abilities to navigate to previous and next pages.
`<path>` is the path to inode.

`--cursor <cursor-token>`

Contains all required information to navigate to previous or next pages. It is base64 string representation of original value that contains direction and node id to request expected page.

Possible value: `bmV4dF8xMjM0`, source: `next_1234`.

### Optional parameters

`--format (text|json)`

Use to specify desired output format. Defaut is `json`.

`--limit <rows-num>`

Set `<rows-num>` as maximum number of rows in the output. Default is **20**.

`--cookie <token>`

Specify `<token>` to be used for the current request. Creation of new session for each request is a costy operation. `<token>` is base64 string representation of the token.

`--all`

Request all available nodes. Pagination is handled by `ls` itself.

### Plain text output

Plain text output should contain following columns:

* type [d/f/S]
* nodid
* size
* path

```
type    node    size        path
[s]     1152    123123      /foo/bar/sometestfile1.txt
...

cookie: c29tZSB0ZXN0IGNvb2tpZQ==
prev cursor: cHJldl8xMjM0
next cursor: bmV4dF8xMjM0
```

### JSON output

Contains `content`, `cursor` and `cookie` as root elements. `content` contains information about inodes. `cursor` contains information required for further pagination. `cookie` contains base64 string representation of session's cookie.

Example:

```json
{
  "content": [
    {
      "Id": 1145,
      "Type": "symlink",
      "Size": 123123,
      "Path": "/foo/bar/sometestfile1.txt",
      "Mode": 509,
      "Uid": 1001,
      "Gid": 1002,
      "ATime": 1670963175158876,
      "MTime": 1670963175158876,
      "CTime": 1670963175158876,
      "Links": 1
    },
    ...
  ],
  "cursor": {
    "previous": null,
    "next": "next_3"
  },
  "cookie": "c29tZSB0ZXN0IGNvb2tpZQ=="
}
```
