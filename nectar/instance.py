"""Local shim for `nectar.instance.shared_blockchain_instance`.

Returns None; this avoids creating a real blockchain instance during unit
tests while preserving the API expected by `hivesbi.memo_parser`.
"""

def shared_blockchain_instance():
    return None
