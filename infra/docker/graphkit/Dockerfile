FROM nvcr.io/nvidia/pyg:26.03-py3

WORKDIR /workspace

RUN pip install --root-user-action=ignore --ignore-installed --no-deps pyyaml==6.0.3 \
    && pip install --root-user-action=ignore numpy==2.2.6 \
    && pip install --root-user-action=ignore mlflow==3.16.1 lightning==2.6.6 seaborn==0.13.2 opt_einsum==3.4.0 \
    && pip install --root-user-action=ignore --no-build-isolation torch-scatter==2.1.2 \
    && pip install --root-user-action=ignore gridfm-datakit==1.1.0 \
    && pip install --root-user-action=ignore --no-deps gridfm-graphkit==0.9.0.3

COPY patch_mlflow_logger.py /tmp/patch_mlflow_logger.py
RUN python3 /tmp/patch_mlflow_logger.py && rm /tmp/patch_mlflow_logger.py

CMD ["bash"]
