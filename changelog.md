# Changelog

**0.2.0:**

- Add API for Redis streams commands:
  - xpending
  - xinfo_groups
  - xinfo_stream
  - xdel
  - xtrim
- Return an empty list when an empty pipeline is awaited.
