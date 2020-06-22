# Changelog

## 0.2.2

- Reset connection (reconnect with Redis) after a call to execute()
  or execute_many() has been cancelled.
- Add xread command.


## 0.2.1

- Remove setup.py shim.


## 0.2.0

- Add API for Redis streams commands:
  - xpending
  - xinfo_groups
  - xinfo_stream
  - xdel
  - xtrim
- Return an empty list when an empty pipeline is awaited.
- Raise ClosedError when connection is unexpectedly closed.
- Use Poetry for dependency management and packaging.
