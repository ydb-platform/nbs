// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

// TODO: Pass in the slice of valid FieldType for each tag.

// Private Tags >= 32768
// Reusable Tags >= 65000

// PrivateTags is exported to allow it to be modified directly with new tags.

var PrivateTags = NewTagSet("Private", 32768, 65535)

func init() {
    /*
        TODO:
        Most of these tags should actually be removed from this package
        and left as an exercise for users to register in their own
        packages. For example, if someone creates a GeoTIFF package, the
        Geo* tags would be defined in that package.  Those tags would be
        registered by importing that package.  We leave these tags here
        for reference and general use until such time as other proper
        packages represent them.
    */

    // Do not lock PrivateTags.  This is meant to be added to by others.

    // http://www.awaresystems.be/imaging/tiff/tifftags/private.html
    PrivateTags.Register(NewTag(32932, "Wang Annotation", nil))
    PrivateTags.Register(NewTag(33445, "MD FileTag", nil))
    PrivateTags.Register(NewTag(33446, "MD ScalePixel", nil))
    PrivateTags.Register(NewTag(33447, "MD ColorTable", nil))
    PrivateTags.Register(NewTag(33448, "MD LabName", nil))
    PrivateTags.Register(NewTag(33449, "MD SampleInfo", nil))
    PrivateTags.Register(NewTag(33450, "MD PrepDate", nil))
    PrivateTags.Register(NewTag(33451, "MD PrepTime", nil))
    PrivateTags.Register(NewTag(33452, "MD FileUnits", nil))
    PrivateTags.Register(NewTag(33723, "IPTC", nil))
    PrivateTags.Register(NewTag(33918, "INGR Packet Data Tag", nil))
    PrivateTags.Register(NewTag(33919, "INGR Flag Registers", nil))
    PrivateTags.Register(NewTag(34377, "Photoshop", nil))
    PrivateTags.Register(NewTag(34675, "ICC Profile", nil))
    PrivateTags.Register(NewTag(34908, "HylaFAX FaxRecvParams", nil))
    PrivateTags.Register(NewTag(34909, "HylaFAX FaxSubAddress", nil))
    PrivateTags.Register(NewTag(34910, "HylaFAX FaxRecvTime", nil))
    PrivateTags.Register(NewTag(37724, "ImageSourceData", nil))
    PrivateTags.Register(NewTag(42112, "GDAL_METADATA", nil))
    PrivateTags.Register(NewTag(42113, "GDAL_NODATA", nil))
    PrivateTags.Register(NewTag(50215, "Oce Scanjob Description", nil))
    PrivateTags.Register(NewTag(50216, "Oce Application Selector", nil))
    PrivateTags.Register(NewTag(50217, "Oce Identification Number", nil))
    PrivateTags.Register(NewTag(50218, "Oce ImageLogic Characteristics", nil))
    PrivateTags.Register(NewTag(50341, "EpsonPrintImageMatching", nil))

    // Alias Sketchbook Pro
    PrivateTags.Register(NewTag(50784, "Alias Layer Metadata", nil))

    DefaultTagSpace.RegisterTagSet(PrivateTags)
}
