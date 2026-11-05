LIBRARY()

VERSION(2.3.1)

LICENSE(LGPL-2.1-or-later)

ORIGINAL_SOURCE(https://download.savannah.nongnu.org/releases/acl/acl-2.3.1.tar.xz)

ADDINCL(
    GLOBAL cloud/filestore/tools/testing/fs_posix_compliance/suite/libacl/include
    cloud/filestore/tools/testing/fs_posix_compliance/suite/libacl/include
    cloud/filestore/tools/testing/fs_posix_compliance/suite/libacl/libacl
)

NO_COMPILER_WARNINGS()
NO_RUNTIME()

SRCS(
    libacl/__acl_extended_file.c
    libacl/__acl_from_xattr.c
    libacl/__acl_reorder_obj_p.c
    libacl/__acl_to_any_text.c
    libacl/__acl_to_xattr.c
    libacl/__apply_mask_to_mode.c
    libacl/__libobj.c
    libacl/acl_add_perm.c
    libacl/acl_calc_mask.c
    libacl/acl_check.c
    libacl/acl_clear_perms.c
    libacl/acl_cmp.c
    libacl/acl_copy_entry.c
    libacl/acl_copy_ext.c
    libacl/acl_copy_int.c
    libacl/acl_create_entry.c
    libacl/acl_delete_def_file.c
    libacl/acl_delete_entry.c
    libacl/acl_delete_perm.c
    libacl/acl_dup.c
    libacl/acl_entries.c
    libacl/acl_equiv_mode.c
    libacl/acl_error.c
    libacl/acl_extended_fd.c
    libacl/acl_extended_file.c
    libacl/acl_extended_file_nofollow.c
    libacl/acl_free.c
    libacl/acl_from_mode.c
    libacl/acl_from_text.c
    libacl/acl_get_entry.c
    libacl/acl_get_fd.c
    libacl/acl_get_file.c
    libacl/acl_get_perm.c
    libacl/acl_get_permset.c
    libacl/acl_get_qualifier.c
    libacl/acl_get_tag_type.c
    libacl/acl_init.c
    libacl/acl_set_fd.c
    libacl/acl_set_file.c
    libacl/acl_set_permset.c
    libacl/acl_set_qualifier.c
    libacl/acl_set_tag_type.c
    libacl/acl_size.c
    libacl/acl_to_any_text.c
    libacl/acl_to_text.c
    libacl/acl_valid.c
    libmisc/high_water_alloc.c
    libmisc/next_line.c
    libmisc/quote.c
    libmisc/unquote.c
)

END()

