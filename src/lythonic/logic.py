# @final
# class Logic:
#     def __init__(self, config: dict[str, Any], default_ref: str | GlobalRef | None = None) -> None:
#         config = dict(config)
#         try:
#             if default_ref is not None:
#                 ref = GlobalRef(config.pop("ref$", default_ref))
#             else:
#                 ref = GlobalRef(config.pop("ref$"))
#             self.async_call = ref.is_async()
#             if ref.is_function():
#                 self.instance = None
#                 self.call = ref.get_instance()
#                 assert config == {}, f"Unexpected entries {config}"
#             elif ref.is_class():
#                 cls = ref.get_instance()
#                 self.call = self.instance = cls(config)
#             else:
#                 raise AssertionError(f"Invalid logic {ref} in config {config}")  # pragma: no cover
#         except BaseException:
#             log.error(f"Error in {config}")
#             raise

