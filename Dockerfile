# Base images and package indexes are build args, for registries other than Docker Hub / npm / PyPI
# (see compose.yaml). The python image is the full one: it has git, so nothing comes from apt.
ARG NODE_IMAGE=node:20-slim
ARG PYTHON_IMAGE=python:3.12

FROM ${NODE_IMAGE} AS frontend
ARG NPM_REGISTRY
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci ${NPM_REGISTRY:+--registry "$NPM_REGISTRY"}
COPY frontend ./
RUN npm run build

FROM ${PYTHON_IMAGE}
ARG PYPI_INDEX
ENV UV_PYTHON_DOWNLOADS=never
RUN pip install -q ${PYPI_INDEX:+--index-url "$PYPI_INDEX"} uv && useradd -u 1000 -m studio
WORKDIR /opt/foundry-studio/backend
# uv.lock pins pypi.org URLs; installing its exported versions goes through PYPI_INDEX instead.
COPY backend/pyproject.toml backend/uv.lock ./
RUN uv export -q --frozen --no-dev --no-emit-project -o requirements.txt \
 && uv venv -q && uv pip install -q ${PYPI_INDEX:+--default-index "$PYPI_INDEX"} -r requirements.txt
COPY backend ./
RUN uv pip install -q ${PYPI_INDEX:+--default-index "$PYPI_INDEX"} --no-deps -e .
COPY vendor /opt/foundry-studio/vendor
COPY --from=frontend /src/dist /opt/foundry-studio/frontend/dist
# OpenShift runs it as an arbitrary UID in group 0.
RUN mkdir -p /opt/foundry-studio/workspace && chown -R 1000:0 /opt/foundry-studio && chmod -R g=u /opt/foundry-studio
USER 1000
ENV STUDIO_WORKSPACE=/opt/foundry-studio/workspace PATH=/opt/foundry-studio/backend/.venv/bin:$PATH
EXPOSE 8000
CMD ["uvicorn", "foundry_studio.app:app", "--host", "0.0.0.0", "--port", "8000"]
