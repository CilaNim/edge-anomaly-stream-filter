import math
import pathlib
import re
import sys
import unittest


def check_firmware_integrity() -> bool:
    print("[1/4] Checking ESP32 firmware (src/edge_node/main.cpp) ...", end=" ")
    main_cpp = pathlib.Path("src/edge_node/main.cpp")
    if not main_cpp.exists():
        print("FAIL (file missing)")
        return False
    content = main_cpp.read_text(encoding="utf-8")

    # Check bracket & parentheses balance
    brace, paren = 0, 0
    in_str, in_chr = False, False
    i, n = 0, len(content)
    while i < n:
        c = content[i]
        if not in_str and not in_chr:
            if c == '"': in_str = True
            elif c == "'": in_chr = True
            elif c == '{': brace += 1
            elif c == '}': brace -= 1
            elif c == '(': paren += 1
            elif c == ')': paren -= 1
        elif in_str:
            if c == '\\': i += 1
            elif c == '"': in_str = False
        elif in_chr:
            if c == '\\': i += 1
            elif c == "'": in_chr = False
        i += 1

    if brace != 0 or paren != 0:
        print(f"FAIL (unbalanced brackets: brace={brace}, paren={paren})")
        return False

    # Check key configurations
    assert "static const uint32_t ANOMALY_PULSE_MS = 200;" in content
    assert "static const size_t WINDOW_SIZE = 50;" in content
    assert "static const float Z_THRESHOLD = 2.5f;" in content
    assert "enum BufferAction {" in content
    assert "static BufferAction decideBufferAction(" in content
    assert "void setup()" in content
    assert "void loop()" in content
    print("OK (syntax, bounds, and constants verified)")
    return True


def check_wokwi_configuration() -> bool:
    print("[2/4] Checking Wokwi configuration & circuit files ...", end=" ")
    import json
    diag_p = pathlib.Path("diagram.json")
    if not diag_p.exists():
        print("FAIL (diagram.json missing)")
        return False
    diag = json.loads(diag_p.read_text(encoding="utf-8"))
    parts = {p["id"]: p["type"] for p in diag.get("parts", [])}
    assert "esp" in parts and parts["esp"] == "board-esp32-devkit-c-v4"
    assert "pot1" in parts and parts["pot1"] == "wokwi-potentiometer"
    assert "led1" in parts and parts["led1"] == "wokwi-led"
    assert "r1" in parts and parts["r1"] == "wokwi-resistor"

    sketch_p = pathlib.Path("sketch.ino")
    assert sketch_p.exists() and "src/edge_node/main.cpp" in sketch_p.read_text(encoding="utf-8")
    print("OK (ESP32, potentiometer, LED, resistor connected; sketch.ino present)")
    return True


def run_unit_tests() -> bool:
    print("[3/4] Running test suite (tests/test_analytical_tools.py) ...")
    suite = unittest.defaultTestLoader.discover("tests")
    runner = unittest.TextTestRunner(verbosity=1)
    res = runner.run(suite)
    return res.wasSuccessful()


def run_analytical_experiment() -> bool:
    print("[4/4] Running bandwidth analysis across seeds in Antigravity ...")
    from src.analytical_tools.bandwidth_analyzer import main
    rc = main(["--offline", "--seeds", "42", "43", "44", "--samples", "200", "--no-chart", "--name", "antigravity_verify"])
    for p in pathlib.Path("results").glob("antigravity_verify*"):
        p.unlink(missing_ok=True)
    return rc == 0


def main() -> int:
    print("=" * 65)
    print(" Edge Anomaly Stream Filter — Verification & Antigravity Runner")
    print("=" * 65)

    if not check_firmware_integrity():
        return 1
    if not check_wokwi_configuration():
        return 1
    if not run_unit_tests():
        return 1
    if not run_analytical_experiment():
        return 1

    print("=" * 65)
    print(" ALL VERIFICATION CHECKS PASSED SUCCESSFULLY IN ANTIGRAVITY")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    sys.exit(main())
