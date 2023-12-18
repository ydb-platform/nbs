KEY_SPEC_PROCESS_KEYRING = -2
KEY_SPEC_USER_KEYRING = -4

KEYRING_TYPE = "keyring"
USER_KEY_TYPE = "user"

USR_ALL_PERM = 0x003f0000


def add_key(keyutils, keyring, desc, data=""):
    key_type = KEYRING_TYPE if data == "" else USER_KEY_TYPE

    key = keyutils.add_key(key_type.encode(), desc.encode(), data, len(data), keyring)
    assert key != -1

    res = keyutils.keyctl_setperm(key, USR_ALL_PERM)
    assert res == 0

    return key


def create_endpoints_keyring(keyutils, root_keyring_name, endpoints_keyring_name):
    root_keyring = add_key(keyutils, KEY_SPEC_PROCESS_KEYRING, root_keyring_name)
    res = keyutils.keyctl_link(root_keyring, KEY_SPEC_USER_KEYRING)
    assert res == 0

    endpoints_keyring = add_key(keyutils, root_keyring, endpoints_keyring_name)
    assert res == 0

    return (root_keyring, endpoints_keyring)


def remove_endpoints_keyring(keyutils, keyring):
    keyutils.keyctl_unlink(keyring, KEY_SPEC_USER_KEYRING)
    keyutils.keyctl_unlink(keyring, KEY_SPEC_PROCESS_KEYRING)
