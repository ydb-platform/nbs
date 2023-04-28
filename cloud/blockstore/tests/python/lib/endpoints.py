import ctypes
import uuid


KEY_SPEC_PROCESS_KEYRING = -2
KEY_SPEC_USER_KEYRING = -4

KEYRING_TYPE = "keyring"
USER_KEY_TYPE = "user"


def __remove_endpoints_keyring(keyring_desc):
    keyutils = ctypes.CDLL('libkeyutils.so.1')

    user_keyring = keyutils.keyctl_search(KEY_SPEC_USER_KEYRING, KEYRING_TYPE, keyring_desc, 0)
    if (user_keyring != -1):
        keyutils.keyctl_unlink(user_keyring, KEY_SPEC_USER_KEYRING)

    proc_keyring = keyutils.keyctl_search(KEY_SPEC_PROCESS_KEYRING, KEYRING_TYPE, keyring_desc, 0)
    if (proc_keyring != -1):
        keyutils.keyctl_unlink(proc_keyring, KEY_SPEC_PROCESS_KEYRING)


def prepare_and_run_test(test_case, test_func):
    keyring_name = str(uuid.uuid4()) + "_nbs_test"

    try:
        return test_func(keyring_name, test_case)
    finally:
        __remove_endpoints_keyring(keyring_name)
