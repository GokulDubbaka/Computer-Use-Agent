from computer_use_demo.security import create_ticket, verify_ticket


def test_desktop_ticket_is_session_bound_and_tamper_evident():
    ticket = create_ticket("sess_a", "secret", ttl_seconds=60)

    assert verify_ticket(ticket, "secret", "sess_a")
    assert not verify_ticket(ticket, "secret", "sess_b")
    assert not verify_ticket(ticket + "tampered", "secret", "sess_a")
    assert not verify_ticket(ticket, "wrong-secret", "sess_a")


def test_expired_desktop_ticket_is_rejected():
    ticket = create_ticket("sess_a", "secret", ttl_seconds=-1)

    assert not verify_ticket(ticket, "secret", "sess_a")
