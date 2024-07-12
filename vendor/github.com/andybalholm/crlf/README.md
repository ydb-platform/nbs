# crlf
--
    import "crlf"

The crlf package helps in dealing with files that have DOS-style CR/LF line
endings.

## Usage

#### func  Create

```go
func Create(name string) (io.WriteCloser, error)
```
Create opens a text file for writing, with platform-appropriate line ending
conversion.

#### func  NewReader

```go
func NewReader(r io.Reader) io.Reader
```
NewReader returns an io.Reader that converts CR or CRLF line endings to LF.

#### func  NewWriter

```go
func NewWriter(w io.Writer) io.Writer
```
NewWriter returns an io.Writer that converts LF line endings to CRLF.

#### func  Open

```go
func Open(name string) (io.ReadCloser, error)
```
Open opens a text file for reading, with platform-appropriate line ending
conversion.

#### type Normalize

```go
type Normalize struct {
}
```

Normalize takes CRLF, CR, or LF line endings in src, and converts them to LF in
dst.

#### func (*Normalize) Reset

```go
func (n *Normalize) Reset()
```

#### func (*Normalize) Transform

```go
func (n *Normalize) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error)
```

#### type ToCRLF

```go
type ToCRLF struct{}
```

ToCRLF takes LF line endings in src, and converts them to CRLF in dst.

#### func (ToCRLF) Reset

```go
func (ToCRLF) Reset()
```

#### func (ToCRLF) Transform

```go
func (ToCRLF) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error)
```
