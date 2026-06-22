import importlib.util
from pathlib import Path


def _load_main():
    # main.py is at repo root (not in a package); load it by path.
    spec = importlib.util.spec_from_file_location("_main_under_test", Path(__file__).resolve().parent.parent / "main.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # main() is guarded by __name__ == "__main__", so this won't run it
    return mod


def test_stop_signal_handler_raises_keyboard_interrupt():
    import signal
    import pytest
    main = _load_main()
    with pytest.raises(KeyboardInterrupt):
        main._handle_stop_signal(signal.SIGTERM, None)


def test_install_registers_sigterm_handler(monkeypatch):
    import signal
    main = _load_main()
    captured = {}
    monkeypatch.setattr(signal, "signal", lambda sig, fn: captured.update(sig=sig, fn=fn))
    main.install_stop_signal_handler()
    assert captured["sig"] == signal.SIGTERM
    assert captured["fn"] is main._handle_stop_signal
