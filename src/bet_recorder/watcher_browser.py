from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SMARKETS_ACTIVE_PORTFOLIO_URL = (
    "https://smarkets.com/portfolio/?time=all&order-state=active"
)
SMARKETS_SETTLED_ACTIVITY_FILTER = "Settled orders"
SMARKETS_ACTIVE_ACTIVITY_FILTER = "All active"


def bootstrap_smarkets_page(*, client, profile_path: Path | None) -> None:
    current_url = client.current_url()
    if _is_smarkets_active_portfolio_url(current_url) or "open-positions" in current_url:
        return
    client.open_url(SMARKETS_ACTIVE_PORTFOLIO_URL)
    client.wait(1500)


def ensure_smarkets_authenticated(*, client) -> None:
    current_url = client.current_url()
    if "login=true" not in current_url:
        return

    credentials = load_smarkets_credentials()
    if credentials is None:
        return

    username, password = credentials
    set_input_value = getattr(client, "set_input_value", None)
    evaluate = getattr(client, "evaluate", None)
    email_selector = 'input[autocomplete="email"], input[name="email"], input[type="email"]'
    password_selector = (
        'input[autocomplete="current-password"], input[name="password"], input[type="password"]'
    )
    submit_selector = 'button[type="submit"]'

    submitted = False
    if callable(set_input_value) and callable(evaluate):
        try:
            set_input_value(email_selector, username)
            set_input_value(password_selector, password)
            evaluate(
                "() => {"
                "const remember = document.querySelector('input[type=\"checkbox\"]');"
                "if (remember instanceof HTMLInputElement && !remember.checked) {"
                "remember.click();"
                "}"
                "const submit = Array.from(document.querySelectorAll('button')).find((element) => {"
                "const text = (element.innerText || element.textContent || '').trim();"
                "return text === 'Log in' && !element.disabled;"
                "}) || Array.from(document.querySelectorAll('button[type=\"submit\"]')).find("
                "(element) => !element.disabled"
                ");"
                "if (!(submit instanceof HTMLButtonElement)) {"
                "throw new Error('Smarkets login submit button not found');"
                "}"
                "if (submit.disabled) {"
                "throw new Error('Smarkets login submit button is still disabled');"
                "}"
                "const form = submit.form || submit.closest('form');"
                "setTimeout(() => {"
                "  if (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') {"
                "    form.requestSubmit(submit);"
                "    return;"
                "  }"
                "  submit.click();"
                "}, 0);"
                "return {"
                "  submitted: true,"
                "  method: (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') ? 'requestSubmit' : 'click',"
                "  rememberMeChecked: !!(remember && remember.checked)"
                "};"
                "}"
            )
            submitted = True
        except Exception:
            submitted = False

    if not submitted:
        client.fill('input[type="email"]', username)
        client.fill('input[type="password"]', password)
        if callable(evaluate):
            evaluate(
                "() => {"
                "const submit = Array.from(document.querySelectorAll('button')).find((element) => {"
                "const text = (element.innerText || element.textContent || '').trim();"
                "return text === 'Log in' && !element.disabled;"
                "}) || Array.from(document.querySelectorAll('button[type=\"submit\"]')).find("
                "(element) => !element.disabled"
                ");"
                "if (!(submit instanceof HTMLButtonElement)) {"
                "throw new Error('Smarkets login submit button not found');"
                "}"
                "const form = submit.form || submit.closest('form');"
                "if (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') {"
                "form.requestSubmit(submit);"
                "return { submitted: true, method: 'requestSubmit' };"
                "}"
                "submit.click();"
                "return { submitted: true, method: 'click' };"
                "}"
            )
        else:
            client.click(submit_selector)

    client.wait(1500)
    for _ in range(8):
        if "login=true" not in client.current_url():
            break
        client.wait(500)

    current_url = client.current_url()
    if "login=true" in current_url:
        client.open_url(SMARKETS_ACTIVE_PORTFOLIO_URL)
        client.wait(1500)


def accept_smarkets_cookies(*, client) -> None:
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return

    accepted = evaluate(
        "(() => {"
        "const button = Array.from(document.querySelectorAll('button')).find((element) => {"
        "const text = (element.innerText || '').trim();"
        "return text === 'Accept all cookies' || text === 'Accept only essential cookies';"
        "});"
        "if (!(button instanceof HTMLButtonElement)) {"
        "return false;"
        "}"
        "button.click();"
        "return true;"
        "})()"
    )
    if accepted:
        client.wait(500)


def ensure_smarkets_activity_filter(
    *, client, target_filter: str = SMARKETS_ACTIVE_ACTIVITY_FILTER
) -> None:
    current_url = client.current_url()
    if not current_url.startswith("https://smarkets.com/portfolio"):
        return
    if not callable(getattr(client, "evaluate", None)):
        return

    current_filter = _current_smarkets_activity_filter(client=client)
    if current_filter == target_filter or current_filter is None:
        return

    client.evaluate(
        "(() => {"
        "const labels = new Set(["
        "'All orders',"
        "'All active',"
        "'Filled orders',"
        "'Unmatched orders',"
        "'Settled orders'"
        "]);"
        f"const targetFilter = {target_filter!r};"
        "const combobox = Array.from(document.querySelectorAll('[role=\"combobox\"]')).find("
        "(element) => labels.has((element.innerText || '').trim())"
        ");"
        "if (!(combobox instanceof HTMLElement)) {"
        "throw new Error('Smarkets activity filter not found');"
        "}"
        "combobox.click();"
        "const clickOption = () => {"
        "const option = Array.from(document.querySelectorAll('[role=\"option\"]')).find("
        "(element) => (element.innerText || '').trim() === targetFilter"
        ");"
        "if (!(option instanceof HTMLElement)) {"
        "setTimeout(clickOption, 50);"
        "return;"
        "}"
        "option.click();"
        "};"
        "setTimeout(clickOption, 0);"
        "return true;"
        "})()"
    )
    client.wait(750)
    current_filter = _current_smarkets_activity_filter(client=client)
    if current_filter != target_filter:
        raise ValueError(
            "Smarkets activity filter is not ready: "
            f"expected {target_filter!r}, got {current_filter!r}"
        )


def load_smarkets_credentials(home: Path | None = None) -> tuple[str, str] | None:
    username = os.environ.get("SMARKETS_USERNAME")
    password = os.environ.get("SMARKETS_PASSWORD")
    if username and password:
        return username, password

    dotenv_path = (home or Path.home()).expanduser() / ".env"
    if not dotenv_path.exists():
        return None

    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')

    username = values.get("SMARKETS_USERNAME")
    password = values.get("SMARKETS_PASSWORD")
    if username and password:
        return username, password
    return None


def _current_smarkets_activity_filter(*, client) -> str | None:
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return None
    return evaluate(
        "(() => {"
        "const labels = new Set(["
        "'All orders',"
        "'All active',"
        "'Filled orders',"
        "'Unmatched orders',"
        "'Settled orders'"
        "]);"
        "const combobox = Array.from(document.querySelectorAll('[role=\"combobox\"]')).find("
        "(element) => labels.has((element.innerText || '').trim())"
        ");"
        "return combobox ? (combobox.innerText || '').trim() : null;"
        "})()"
    )


def _is_smarkets_active_portfolio_url(url: str) -> bool:
    if not url.startswith("https://smarkets.com/portfolio"):
        return False
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return (
        query.get("order-state", [None])[0] == "active"
        and query.get("time", [None])[0] == "all"
    )
