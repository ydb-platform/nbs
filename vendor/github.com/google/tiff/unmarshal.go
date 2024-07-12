// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "encoding/binary"
    "fmt"
    "log"
    "math/big"
    "reflect"
    "strconv"
    "strings"
    "time"
)

type keyValPair struct {
    Key string
    Val string
}

/*
TIFF Struct Tag:
  Representation(s):
    `tiff:"type"`
    `tiff:"type,data"`
  Notes:
    1. A tiff struct tag represents the general format of a tiff struct tag.
    2. The first part is always a simple alpha-numeric only type reference.
    3. Current supported types are:
         ifd
         subifd
         field
    4. The type is separated by the data portion by a single ',' character.
    5. The data portion is just a string that usually contains some structure
       referenced by the type.
*/

type tiffStructTag struct {
    Type string
    Data string
}

func (tst tiffStructTag) String() string {
    if tst.Type == "" {
        return ""
    }
    if tst.Data == "" {
        return fmt.Sprintf(`tiff:"%s"`, tst.Type)
    }
    return fmt.Sprintf(`tiff:"%s,%s"`, tst.Type, tst.Data)
}

func ParseTiffStructTag(text string) *tiffStructTag {
    if len(text) == 0 {
        return nil
    }
    pair := strings.SplitN(text, ",", 2)
    switch len(pair) {
    case 1:
        return &tiffStructTag{
            Type: pair[0],
        }
    case 2:
        return &tiffStructTag{
            Type: pair[0],
            Data: pair[1],
        }
    }
    return nil
}

/*
IFD Field Struct Tag:
  Representation(s):
    `tiff:"field,tag=%d,typ=%d,cnt=%d,off=(true|false),def=[%v,%v,%v]"`
  Notes:
    1. A tiff field struct tag starts with "field" followed by a ',' and then
       one or more key value pairs in the form key=value.
    2. The key value pairs are separated by commas.
    3. There are five key types.
       3.1. tag: Represents the IFD Field's numeric Tag ID value.
       3.2. typ: Represents the IFD Field's numeric Field Type value.
       3.3. cnt: Represents the IFD Field's numeric Count value.
       3.4. off: Indicates if the value portion of the underlying entry is
                 actually an offset to the bytes found in a Field's value.
       3.5. def: Represents the value(s) that will be used in the event that an
                 ifd does not contain a Field that associates with this struct
                 field.
    4. Notes about the key "tag".
       4.1. This is the only REQUIRED key.
       4.2. The value of the "tag" key MUST be in base10 and MUST fit into a
            uint16.
    5. Notes about the key "typ".
       5.1. This is an OPTIONAL key UNLESS a "def" key exists.
       5.2. The value of the "typ" key MUST be in base10 and MUST fit into a
            uint16.
    6. Notes about the key "cnt".
       6.1. This is an OPTIONAL key UNLESS a "def" key exists with multiple
            values and the struct field's type is a slice.  If the struct field
            type is an array, the expected count is taken from the array's
            length.
       6.2. The value of the "cnt" key MUST be in base10 and MUST fit into a
            uint64.
    7. Notes about the key "off".
       7.1. This is an OPTIONAL key.
       7.2. The value of the "off" key MUST be able to be parsed by
            strconv.ParseBool (found at
            http://golang.org/pkg/strconv/#ParseBool).
    8. Notes about the key "def".
       8.1. This is an OPTIONAL key.
       8.2. A "typ" key is REQUIRED if a "def" key is present.  A "cnt" key is
            REQUIRED if the struct field's type is a slice.
       8.3. The value of "def" is a string representation of a default value
            that a tag may have (often indicated in documentation).
       8.4. There are a few rules for the structure of the "def" key's value.
            If the rules for the are not followed, parsing will silently fail.
    9. Rules for the format of the value of the "def" key.
       9.1. The key name for the default field is "def" (without quotes).
       9.2. A def key SHOULD be placed at the end of the text sequence, but MAY
            exist anywhere past the starting "field,".
       9.3. All values for def MUST start with [ and end with ].  The
            contents in between will be used.  A tiff field struct tag has no
            other uses for [].
       9.4. There are 4 representations supported for parsing of default values.
            9.4.1. Byte, Undefined, and Integers: MUST be in base10 (decimal).
                       1, -23, 456, 7890
            9.4.2. Rationals: MUST have the form x/y where x and y are both
                   base10.
                       1/2, -3/4, 5/-6, -7/-8
            9.4.3. Floats & Doubles: The form "-1234.5678" (without quotes)
            9.4.4. ASCII MUST follow within the bounds of a go string literal.
                   Specifically,
                   http://golang.org/ref/spec#interpreted_string_lit is what
                   MUST be used.  However, for the case of a literal back quote,
                   it SHOULD be represented in its hexadecimal form within the
                   string.   The ` character SHOULD be written as \x60 since the
                   struct tags themselves are raw strings that use ``.  The
                   octal form \140 MAY also be used.
                       ` == \x60 // Hexadecimal form of Back Quote/Back Tick
                       ` == \140 // Octal form of Back Quote/Back Tick
                   Example:
                       Given the following struct tag...
                           `tiff:"field,tag=1,def=[Some quotes \" and ' are easy, but \x60 is more involved.]"`
                       Running reflect's StructTag.Get("tiff") will return...
                           "field,tag=1,def=[Some quotes \" and ' are easy, but ` is more involved.]"
                       And the default value will end up being...
                           "Some quotes \" and ' are easy, but ` is more involved."
                       When printed to stdout with fmt.Println...
                           Some quotes " and ' are easy, but ` is more involved.
                   If def=[] is present for ascii, an empty string is
                   assumed.  If you do not wish for the string to be set (i.e.
                   you are using a *string and want it to remain nil), then do
                   not specify default in the struct tag.  For all other types,
                   using def=[] will not set the zero value.
       9.5. For counts > 1 for all types other than ASCII, use a ',' to separate
            values.
                byte, undefined, integers: def=[1,78,255,0,42]
                rationals:                 def=[1/2,-3/4,5/-6,-7/-8]
                floats & doubles:          def=[-1.234,56.78,9.0]
       9.6. If the struct field is not an array or slice, but you specify
            multiple values as though count > 1, only the first value will be
            used.
*/

// A fieldStructTag is a data representation of a struct tag used for a struct
// field that directly relates to a tiff ifd field.
type fieldStructTag struct {
    Tag     *uint16 // key name "tag"
    Type    *uint16 // key name "typ"
    Count   *uint64 // key name "cnt"
    Offset  *bool   // key name "off"
    Default *string // key name "def"
}

func (fst fieldStructTag) String() string {
    var pairs []string
    if fst.Tag != nil {
        pairs = append(pairs, fmt.Sprintf("tag=%d", *fst.Tag))
    }
    if fst.Type != nil {
        pairs = append(pairs, fmt.Sprintf("typ=%d", *fst.Type))
    }
    if fst.Count != nil {
        pairs = append(pairs, fmt.Sprintf("cnt=%d", *fst.Count))
    }
    if fst.Offset != nil {
        pairs = append(pairs, fmt.Sprintf("off=%v", *fst.Offset))
    }
    if fst.Default != nil {
        quoted := strconv.QuoteToASCII(*fst.Default)
        quoted = quoted[1 : len(quoted)-1] // removes the quotes, but keeps the representation
        pairs = append(pairs, fmt.Sprintf("def=[%s]", quoted))
    }
    if len(pairs) > 0 {
        return "field," + strings.Join(pairs, ",")
    }
    return ""
}

func ParseTiffFieldStructTag(text string) (out *fieldStructTag) {
    if len(text) < 5 {
        return nil
    }

    pairs := make([]keyValPair, 0, 5)
    nextPair := new(keyValPair)
    var key *string
    var val *string

    for i := 0; i < len(text); {
        switch {
        case key == nil:
            if len(text[i:]) < 5 {
                return
            }
            if text[i+3] != '=' {
                return
            }
            nextKey := text[i : i+3]
            i += 4
            key = &nextKey
        case val == nil:
            switch *key {
            case "def":
                if text[i] != '[' {
                    return
                }
                to := strings.LastIndex(text, "]")
                if to == -1 {
                    return
                }
                valText := text[i+1 : to] // Remove the [ ]
                val = &valText
                i = to + 1
                if i < len(text) {
                    if text[i] == ',' {
                        i += 1 // pass the comma
                    }
                }
            case "tag", "typ", "cnt", "off":
                j := strings.Index(text[i:], ",")
                if j == -1 { // Assume we must be at the end
                    valText := text[i:]
                    val = &valText
                    i = len(text)
                } else {
                    valText := text[i : i+j]
                    val = &valText
                    i += j + 1 // move i to 1 past the ,
                }
            default:
                // Invalid key name
                return
            }
        }
        if key != nil && val != nil {
            nextPair.Key = *key
            nextPair.Val = *val
            pairs = append(pairs, *nextPair)
            nextPair = new(keyValPair)
            key = nil
            val = nil
        }
    }

    if len(pairs) == 0 {
        return nil
    }

    var fst fieldStructTag
    var hasValue bool
    for _, pair := range pairs {
        switch pair.Key {
        case "tag":
            tagInt, err := strconv.ParseUint(pair.Val, 10, 16)
            if err != nil {
                log.Printf("tiff: structtag key/val conversion failure for key \"tag\": %v\n", err)
                continue
            }
            tagID := uint16(tagInt)
            fst.Tag = &tagID
            hasValue = true
        case "typ":
            tInt, err := strconv.ParseUint(pair.Val, 10, 16)
            if err != nil {
                log.Printf("tiff: structtag key/val conversion failure for key \"typ\": %v\n", err)
                continue
            }
            t := uint16(tInt)
            fst.Type = &t
            hasValue = true
        case "cnt":
            cInt, err := strconv.ParseUint(pair.Val, 10, 64)
            if err != nil {
                log.Printf("tiff: structtag key/val conversion failure for key \"cnt\": %v\n", err)
                continue
            }

            fst.Count = &cInt
            hasValue = true
        case "off":
            o, err := strconv.ParseBool(pair.Val)
            if err != nil {
                log.Printf("tiff: structtag key/val conversion failure for key \"off\": %v\n", err)
                continue
            }
            fst.Offset = &o
            hasValue = true
        case "def":
            // Make a copy or we risk the value being changed with
            // each iteration since pair is reused with the same
            // memory location each time through the loop.
            defVal := pair.Val
            fst.Default = &defVal
            hasValue = true
        }
    }
    if !hasValue {
        return
    }
    return &fst
}

/*
IFD Struct Tag:
  Representation(s):
    `tiff:"ifd"`
    `tiff:"ifd,idx=%d"`
  Notes:
    1. A tiff ifd struct tag starts with "ifd" followed by a ',' and then
       zero or one key value pair in the form key=value.
    2. There is only one key type.
       2.1. idx: Represents the index location of the IFD from TIFF.IFDs().
    3. Notes about the key "idx".
       3.1. This is an OPTIONAL key.
       3.2. The value of the "idx" key MUST be in base10 and and MUST fit into
            an int.
       3.3. Negative values are ignored.  Only values >= 0 are used.
       3.4. The absence of the idx key assumes a value of 0 when performing tiff
            unmarshaling.
       3.5. When performing IFD unmarshaling, if an ifd is being unmarshaled
            into a struct that contains either a field or an embedding that is a
            struct or pointer to a struct and that field has a tiff ifd struct
            tag, the ifd being unmarshaled is used again for the sub struct.
            Any idx key and its value will be ignored.
    4. The purpose of allowing struct fields nested inside structs (that already
       represent ifds) to use tiff ifd struct tags, is for the case where
       someone chooses to break up their IFD representation into separate types
       each with different fields from the same IFD.  For example, one may
       choose to have one struct to hold storage based values, another struct
       for meta data fields, and another for image representation.  This has the
       side effect of not preventing the same fields to be copied multiple
       times.  We do not prevent programmers from doing bad things with this
       (i.e. multiple layers of nesting with the same fields), but we hope
       anyone working in such a manner knows what they are doing.
*/

type ifdStructTag struct {
    Index *int
}

func ParseTiffIFDStructTag(text string) *ifdStructTag {
    if len(text) < 5 {
        return nil
    }
    if !strings.HasPrefix(text, "idx=") {
        return nil
    }
    idxValText := text[4:]
    idx64, err := strconv.ParseInt(idxValText, 10, 64)
    if err != nil {
        log.Printf("tiff: structtag key/val conversion failure for key \"idx\": %v\n", err)
        return nil
    }
    idx := int(idx64)
    return &ifdStructTag{&idx}
}

/*
Sub-IFD Struct Tag:
  Representation:
    `tiff:"subifd,tag=330,idx=0"`
    `tiff:"subifd,tag=330,idx=1"`
    `tiff:"subifd,tag=330,idx=2"`
    `tiff:"subifd,tag=34665,tsp=%s"`
    `tiff:"subifd,tag=34853,tsp=%s"`
  Notes:
    ...
*/

type subIFDStructTag struct {
    Tag      *uint16
    Index    *int
    TagSpace *string
}

func ParseTiffSubIFDStructTag(text string) *subIFDStructTag {
    pairs := strings.Split(text, ",")
    if len(pairs[0]) < 5 {
        return nil
    }
    if pairs[0][:4] != "tag=" {
        return nil
    }
    tagValText := pairs[0][4:]
    tag64, err := strconv.ParseUint(tagValText, 10, 16)
    if err != nil {
        log.Printf("tiff: structtag key/val conversion failure for key \"tag\": %v\n", err)
        return nil
    }
    tagNum := uint16(tag64)
    var sist subIFDStructTag
    sist.Tag = &tagNum

    if len(pairs) > 1 {
        for _, p := range pairs[1:] {
            if len(p) < 5 {
                return nil
            }
            switch p[:4] {
            case "idx=":
                idxValText := pairs[1][4:]
                idx64, err := strconv.ParseInt(idxValText, 10, 64)
                if err != nil {
                    log.Printf("tiff: structtag key/val conversion failure for key \"idx\": %v\n", err)
                    return nil
                }
                idx := int(idx64)
                sist.Index = &idx
            case "tsp=":
                tspText := string(pairs[1][4:])
                sist.TagSpace = &tspText
            }
        }
    }
    return &sist
}

var bigRatType = reflect.TypeOf((*big.Rat)(nil))
var timeType = reflect.TypeOf(time.Time{})

type ErrUnsuppConversion struct {
    From FieldType
    To   reflect.Type
}

func (e ErrUnsuppConversion) Error() string {
    return fmt.Sprintf("tiff: unmarshal: no support for converting field type %q (id: %d) to %q", e.From.Name(), e.From.ID(), e.To)
}

type ErrUnsuppStructField struct {
    T       reflect.Type
    Field   int
    Problem string
}

func (e ErrUnsuppStructField) Error() string {
    var msg string
    if e.Problem != "" {
        msg = ": " + e.Problem
    }
    return fmt.Sprintf("tiff: unmarshal: error for field %s of type %s%s", e.T.Field(e.Field).Name, e.T.Name(), msg)
}

func unmarshalVal(data []byte, bo binary.ByteOrder, ft FieldType, v reflect.Value) error {
    // Do the quick and simple thing.
    if v.Type() == ft.ReflectType() {
        v.Set(ft.Valuer()(data, bo))
        return nil
    }

    typ := v.Type()
    switch typ.Kind() {
    case reflect.Ptr:
        switch typ {
        case bigRatType:
            // If the types were the same, it should have been caught above.
            return ErrUnsuppConversion{ft, typ}
        default:
            newV := reflect.New(typ.Elem())
            if err := unmarshalVal(data, bo, ft, newV.Elem()); err != nil {
                return err
            }
            v.Set(newV)
        }
    case reflect.String:
        var s string
        switch ft.ReflectType().Kind() {
        case reflect.Uint8:
            s = string(data)
        default:
            return ErrUnsuppConversion{ft, typ}
        }
        v.SetString(s)

    case reflect.Uint16:
        // We can up convert an uint8/byte to a uint16.
        if ft.ReflectType().Kind() == reflect.Uint8 {
            v.SetUint(uint64(data[0]))
        } else {
            return ErrUnsuppConversion{ft, typ}
        }
    case reflect.Int16:
        // We can up convert an int8 to an int16.
        if ft.ReflectType().Kind() == reflect.Int8 {
            v.SetInt(int64(int8(data[0])))
        } else {
            return ErrUnsuppConversion{ft, typ}
        }
    case reflect.Uint32:
        var u32 uint32
        switch ft.ReflectType().Kind() {
        case reflect.Uint16:
            // We can up convert an uint16 to an uint32.
            u32 = uint32(bo.Uint16(data))
        case reflect.Uint8:
            // We can up convert an uint8 to an uint32.
            u32 = uint32(data[0])
        default:
            return ErrUnsuppConversion{ft, typ}
        }
        v.SetUint(uint64(u32))
    case reflect.Int32:
        var i32 int32
        switch ft.ReflectType().Kind() {
        case reflect.Int16:
            // We can up convert an int16 to an int32.
            i32 = int32(int16(bo.Uint16(data)))
        case reflect.Int8:
            // We can up convert an int8 to an int32.
            i32 = int32(int8(data[0]))
        default:
            return ErrUnsuppConversion{ft, typ}
        }
        v.SetInt(int64(i32))
    case reflect.Uint64:
        var u64 uint64
        switch ft.ReflectType().Kind() {
        case reflect.Uint32:
            u64 = uint64(bo.Uint32(data))
        case reflect.Uint16:
            u64 = uint64(bo.Uint16(data))
        case reflect.Uint8:
            u64 = uint64(data[0])
        default:
            return ErrUnsuppConversion{ft, typ}
        }
        v.SetUint(u64)
    case reflect.Int64:
        var i64 int64
        switch ft.ReflectType().Kind() {
        case reflect.Int32:
            i64 = int64(int32(bo.Uint32(data)))
        case reflect.Int16:
            i64 = int64(int16(bo.Uint16(data)))
        case reflect.Int8:
            i64 = int64(int8(data[0]))
        default:
            return ErrUnsuppConversion{ft, typ}
        }
        v.SetInt(i64)
    case reflect.Uint8, reflect.Int8, reflect.Float32, reflect.Float64:
        // If this was not handled at the top, we do not support
        // converting other types to these types.
        return ErrUnsuppConversion{ft, typ}
    }
    return nil
}

func UnmarshalIFD(ifd IFD, out interface{}) error {
    if len(ifd.Fields()) == 0 {
        return fmt.Errorf("tiff: UnmarshalIFD: ifd has no fields")
    }
    v := reflect.ValueOf(out).Elem()
    structType := v.Type()

    for i := 0; i < v.NumField(); i++ {
        stField := structType.Field(i)
        sTag := ParseTiffStructTag(stField.Tag.Get("tiff"))
        if sTag == nil {
            continue
        }
        if sTag.Type != "ifd" && sTag.Type != "field" {
            continue
        }

        vf := v.Field(i)
        vft := vf.Type()
        vftk := vft.Kind()

        switch sTag.Type {
        case "ifd":
            // Ignore the data contents of the ifd tag, it is used
            // when parsing whole tiffs (i.e. where the index value
            // is useful).

            switch vftk {
            case reflect.Ptr:
                // We do not support recursive unmarshaling when
                // the field points back to the enclosing struct.
                if vf.Elem() != v && vft.Elem().Kind() == reflect.Struct {
                    newStruct := reflect.New(vft.Elem())
                    if err := UnmarshalIFD(ifd, newStruct.Interface()); err != nil {
                        return err
                    }
                    vf.Set(newStruct)
                }
            case reflect.Struct:
                embStructPtr := v.Field(i).Addr().Interface()
                if err := UnmarshalIFD(ifd, embStructPtr); err != nil {
                    return err
                }
            default:
                log.Printf("tiff: UnmarshalIFD: using a tiff ifd struct tag is only supported for structs (not a %v)\n", vftk)
                continue
            }
        case "field":
            fTag := ParseTiffFieldStructTag(sTag.Data)
            if fTag == nil {
                log.Printf("tiff: UnmarshalIFD: skipping struct field %q due to malformed tiff field struct tag (%q).\n", stField.Name, sTag.Data)
                continue
            }
            if fTag.Tag == nil {
                log.Printf("tiff: UnmarshalIFD: skipping struct field %q due to missing \"tag\" key in tiff field struct tag.\n", stField.Name)
                continue
            }
            if !ifd.HasField(*fTag.Tag) {
                // TODO(jonathanpittman): Check for default values and use those.
                if fTag.Default != nil {
                    if fTag.Type == nil {
                        log.Printf("tiff: UnmarshalIFD: skipping default unmarshaling for struct field %q due to missing \"typ\" key.\n", stField.Name)
                        continue
                    }
                    //defVal := *fTag.Default
                    //ft := DefaultFieldTypeSpace.GetFieldType(*fTag.Type)
                }
            } else {
                // ifd field setup
                ifdField := ifd.GetField(*fTag.Tag)
                ifdFT := ifdField.Type()
                fvBytes := ifdField.Value().Bytes()
                fvBo := ifdField.Value().Order()

                switch vftk {
                case reflect.Array:
                    l := vft.Len()
                    buf := fvBytes[:]
                    for j := 0; j < l; j++ {
                        data := buf[:ifdFT.Size()]
                        if err := unmarshalVal(data, fvBo, ifdFT, vf.Index(j)); err != nil {
                            return err
                        }
                        buf = buf[ifdFT.Size():]
                    }
                case reflect.Slice:
                    newSlice := reflect.MakeSlice(vft, int(ifdField.Count()), int(ifdField.Count()))
                    l := newSlice.Len()
                    buf := fvBytes[:]
                    for j := 0; j < l; j++ {
                        data := buf[:ifdFT.Size()]
                        if err := unmarshalVal(data, fvBo, ifdFT, newSlice.Index(j)); err != nil {
                            return err
                        }
                        buf = buf[ifdFT.Size():]
                    }
                    vf.Set(newSlice)
                default:
                    if err := unmarshalVal(fvBytes, fvBo, ifdFT, vf); err != nil {
                        return err
                    }
                }
            }
        }
    }
    return nil
}

func UnmarshalSubIFDs(ifd IFD, br BReader, tsp TagSpace, out interface{}) error {
    if br == nil {
        return fmt.Errorf("tiff: UnmarshalSubIFDs: no BReader available")
    }
    if len(ifd.Fields()) == 0 {
        return fmt.Errorf("tiff: UnmarshalSubIFDs: ifd has no fields")
    }
    v := reflect.ValueOf(out).Elem()
    structType := v.Type()

    for i := 0; i < v.NumField(); i++ {
        stField := structType.Field(i)
        sTag := ParseTiffStructTag(stField.Tag.Get("tiff"))
        if sTag == nil || sTag.Type != "subifd" {
            continue
        }
        vf := v.Field(i)
        vft := vf.Type()
        vftk := vft.Kind()

        siTag := ParseTiffSubIFDStructTag(sTag.Data)
        if siTag == nil {
            log.Printf("tiff: UnmarshalSubIFDs: skipping struct field %q due to malformed tiff subifd struct tag (%q).\n", stField.Name, sTag.Data)
            continue
        }
        if siTag.Tag == nil {
            log.Printf("tiff: UnmarshalSubIFDs: skipping struct field %q due to missing \"tag\" key in tiff subifd struct tag.\n", stField.Name)
            continue
        }
        if !ifd.HasField(*siTag.Tag) {
            // Default values do not work here.  We have to simply skip it.
            continue
        }
        ifdField := ifd.GetField(*siTag.Tag)
        ifdFT := ifdField.Type()
        fvBytes := ifdField.Value().Bytes()
        fvBo := ifdField.Value().Order()
        if ifdField.Count() == 0 {
            continue
        }
        if ifdFT.ReflectType().Kind() != reflect.Uint32 {
            continue
        }

        offsets := make([]uint64, ifdField.Count())
        for i := range offsets {
            offsets[i] = uint64(fvBo.Uint32(fvBytes))
            fvBytes = fvBytes[4:]
        }

        off := offsets[0]
        if siTag.Index != nil {
            if *siTag.Index >= len(offsets) || *siTag.Index < 0 {
                // log a warning
                continue
            }
            off = offsets[*siTag.Index]
        }

        if tsp == nil {
            if siTag.TagSpace != nil {
                newSpace := GetTagSpace(*siTag.TagSpace)
                if newSpace != nil {
                    tsp = newSpace
                }
            }
            if tsp == nil {
                tsp = DefaultTagSpace
            }
        }

        subIFD, err := ParseIFD(br, uint64(off), tsp, nil)
        if err != nil {
            return err
        }
        switch vftk {
        case reflect.Ptr:
            // We do not support recursive unmarshaling when
            // the field points back to the enclosing struct.
            if vf.Elem() != v && vft.Elem().Kind() == reflect.Struct {
                newStruct := reflect.New(vft.Elem())
                if err := UnmarshalIFD(subIFD, newStruct.Interface()); err != nil {
                    return err
                }
                vf.Set(newStruct)
            }
        case reflect.Struct:
            embStructPtr := v.Field(i).Addr().Interface()
            if err := UnmarshalIFD(subIFD, embStructPtr); err != nil {
                return err
            }
        default:
            log.Printf("tiff: UnmarshalSubIFDs: using a tiff SubIFD struct tag is only supported for structs (not a %v)\n", vftk)
            continue
        }
    }
    return nil
}

func UnmarshalTIFF(t TIFF, out interface{}) error {
    if t == nil {
        return fmt.Errorf("tiff: UnmarshalTIFF: nil TIFF value")
    }
    if len(t.IFDs()) == 0 {
        return fmt.Errorf("tiff: UnmarshalTIFF: no IFDs found")
    }

    v := reflect.ValueOf(out).Elem()
    structType := v.Type()
    for i := 0; i < v.NumField(); i++ {
        stField := structType.Field(i)
        sTag := ParseTiffStructTag(stField.Tag.Get("tiff"))
        if sTag == nil || sTag.Type != "ifd" {
            continue
        }

        ifdIdx := 0 // Default of 0, unless an index key is present.
        iTag := ParseTiffIFDStructTag(sTag.Data)
        if iTag != nil && iTag.Index != nil && *iTag.Index > 0 {
            ifdIdx = *iTag.Index
        }
        if ifdIdx >= len(t.IFDs()) {
            log.Printf("tiff: UnmarshalTIFF: ifd struct tag index out of range for this tiff: %d > %d\n", ifdIdx, len(t.IFDs()))
            continue
        }
        ifd := t.IFDs()[ifdIdx]

        vf := v.Field(i)
        vft := vf.Type()
        vftk := vft.Kind()

        switch vftk {
        case reflect.Ptr:
            if vf.Elem() != v && vft.Elem().Kind() == reflect.Struct {
                newStruct := reflect.New(vft.Elem())
                if err := UnmarshalIFD(ifd, newStruct.Interface()); err != nil {
                    return err
                }
                vf.Set(newStruct)
            }
        case reflect.Struct:
            embStructPtr := v.Field(i).Addr().Interface()
            if err := UnmarshalIFD(ifd, embStructPtr); err != nil {
                return err
            }
        }
    }
    return nil
}
