import lightning.pytorch.loggers.mlflow as mlflow_logger_module

path = mlflow_logger_module.__file__
with open(path) as f:
    content = f.read()

old = """        if self._tracking_uri.startswith(LOCAL_FILE_URI_PREFIX):
            return self._tracking_uri[len(LOCAL_FILE_URI_PREFIX) :]
        return None"""

new = """        if self._tracking_uri.startswith(LOCAL_FILE_URI_PREFIX):
            return self._tracking_uri[len(LOCAL_FILE_URI_PREFIX) :]
        return "mlruns\""""

with open(path, "w") as f:
    f.write(content.replace(old, new))
