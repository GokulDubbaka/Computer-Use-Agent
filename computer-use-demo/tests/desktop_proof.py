from __future__ import annotations

import http.cookiejar
import json
import time
import urllib.error
import urllib.request

from websockets.sync.client import connect

BASE_URL = "http://127.0.0.1:8080"


def main() -> None:
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    session_ids: list[str] = []
    try:
        for title in ("desktop proof A", "desktop proof B"):
            create = urllib.request.Request(
                BASE_URL + "/api/sessions",
                data=json.dumps({"title": title}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(create, timeout=90) as response:
                session_ids.append(json.load(response)["id"])
        session_id, other_session_id = session_ids
        time.sleep(4)
        ticket_request = urllib.request.Request(
            BASE_URL + f"/api/sessions/{session_id}/desktop-ticket",
            data=b"",
            method="POST",
        )
        with opener.open(ticket_request, timeout=30):
            pass
        cookie = "; ".join(f"{item.name}={item.value}" for item in jar)
        cross_request = urllib.request.Request(
            BASE_URL + f"/desktop/{other_session_id}/vnc.html",
            headers={"Cookie": cookie},
        )
        try:
            urllib.request.urlopen(cross_request, timeout=20)
            cross_session_denied = False
        except urllib.error.HTTPError as exc:
            cross_session_denied = exc.code == 401
        with connect(
            f"ws://127.0.0.1:8080/desktop/{session_id}/websockify",
            additional_headers={"Cookie": cookie},
            subprotocols=["binary"],
            open_timeout=20,
        ) as websocket:
            banner = websocket.recv(timeout=20)
        assert isinstance(banner, bytes) and banner.startswith(b"RFB "), banner
        assert cross_session_denied
        print(
            json.dumps(
                {
                    "desktop_websocket": "passed",
                    "cross_session_denied": True,
                    "session_id": session_id,
                    "banner": banner.decode().strip(),
                }
            )
        )
    finally:
        for session_id in session_ids:
            delete = urllib.request.Request(
                BASE_URL + f"/api/sessions/{session_id}", method="DELETE"
            )
            try:
                with opener.open(delete, timeout=30):
                    pass
            except urllib.error.HTTPError:
                pass


if __name__ == "__main__":
    main()
