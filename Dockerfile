FROM python:3.12-slim-bookworm AS dev

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

ENV PATH="/root/.local/bin:$PATH"

COPY . /mp-dwh
COPY pyproject.toml uv.lock /mp-dwh/

WORKDIR /mp-dwh

RUN uv sync --frozen --no-cache

ENV PATH="/mp-dwh/.venv/bin:$PATH"

CMD ["sh", "-c", "uv run src/workflow.py full && uv run src/workflow.py report"]
