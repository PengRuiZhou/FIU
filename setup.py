"""Minimal setup.py for Rust extension build via setuptools-rust."""
import os
from setuptools import setup

# Only build Rust extension if setuptools-rust is installed AND
# the order_accel/ directory exists (i.e., Rust source is present).
# Use __file__ for absolute path — works regardless of CWD.
_here = os.path.dirname(os.path.abspath(__file__))
rust_exts = []
if os.path.isdir(os.path.join(_here, "order_accel")):
    try:
        from setuptools_rust import RustExtension
        rust_exts = [
            RustExtension(
                "minute_bar._order_accel",
                path="order_accel/Cargo.toml",
            )
        ]
    except ImportError:
        # Rust source exists but setuptools-rust not installed — warn the developer.
        # This prevents silent "no extension" confusion when a colleague clones the repo.
        import sys
        print(
            "WARNING: order_accel/ directory found but setuptools-rust not installed. "
            "Rust extension will NOT be built. Install with: pip install setuptools-rust",
            file=sys.stderr,
        )

setup(rust_extensions=rust_exts)
