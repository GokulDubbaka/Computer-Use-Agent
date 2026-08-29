FROM docker.io/debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DISPLAY_NUM=1 \
    DISPLAY=:1 \
    WIDTH=1024 \
    HEIGHT=768

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    dbus-x11 \
    firefox-esr \
    fonts-dejavu-core \
    imagemagick \
    netcat-openbsd \
    novnc \
    openbox \
    procps \
    python3 \
    python3-pip \
    python3-venv \
    scrot \
    sudo \
    supervisor \
    tigervnc-standalone-server \
    websockify \
    x11-utils \
    xdotool \
    xterm \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash computeruse \
    && echo "computeruse ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/computeruse \
    && chmod 0440 /etc/sudoers.d/computeruse \
    && mkdir -p /tmp/.X11-unix \
    && chmod 1777 /tmp/.X11-unix \
    && python3 -m venv /opt/venv \
    && chown -R computeruse:computeruse /opt/venv

ENV HOME=/home/computeruse \
    PATH=/opt/venv/bin:$PATH \
    SESSION_DB_PATH=/home/computeruse/.anthropic/sessions.sqlite3

WORKDIR /home/computeruse
USER computeruse

COPY --chown=computeruse:computeruse computer_use_demo/requirements.txt ./computer_use_demo/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r ./computer_use_demo/requirements.txt

COPY --chown=computeruse:computeruse image/ ./
COPY --chown=computeruse:computeruse computer_use_demo/ ./computer_use_demo/

RUN mkdir -p "$HOME/.anthropic" \
    && chmod +x ./entrypoint.sh ./start_tigervnc.sh ./start_openbox.sh ./start_novnc.sh

EXPOSE 8080 6080 5900

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["./entrypoint.sh"]
