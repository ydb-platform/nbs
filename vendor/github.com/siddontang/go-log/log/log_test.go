package log

import (
    "os"
    "testing"
)

func TestStdStreamLog(t *testing.T) {
    h, _ := NewStreamHandler(os.Stdout)
    s := NewDefault(h)
    s.Info("hello world")

    s.Close()

    Info("hello world")

    defer func() {
        recover()
    }()

    Panic("hello wrold")
}

func TestRotatingFileLog(t *testing.T) {
    path := "./test_log"
    os.RemoveAll(path)

    os.Mkdir(path, 0777)
    fileName := path + "/test"

    h, err := NewRotatingFileHandler(fileName, 10, 2)
    if err != nil {
        t.Fatal(err)
    }

    buf := make([]byte, 10)

    h.Write(buf)

    h.Write(buf)

    if _, err := os.Stat(fileName + ".1"); err != nil {
        t.Fatal(err)
    }

    if _, err := os.Stat(fileName + ".2"); err == nil {
        t.Fatal(err)
    }

    h.Write(buf)
    if _, err := os.Stat(fileName + ".2"); err != nil {
        t.Fatal(err)
    }

    h.Close()

    os.RemoveAll(path)
}
