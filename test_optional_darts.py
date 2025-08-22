#!/usr/bin/env python3
"""
Test script to verify that darts is properly configured as an optional dependency.
This script should work even when darts is not installed.
"""


def test_basic_imports():
    """Test that basic imports work without darts."""
    try:
        from tsdb.decorators.pydantic_decorator import db_crud  # noqa: F401

        print("✓ Basic CRUD decorator imports successfully")
        return True
    except ImportError as e:
        print(f"✗ Basic import failed: {e}")
        return False


def test_darts_optional_import():
    """Test that darts imports are handled gracefully."""
    try:
        from tsdb.decorators.darts_decorator import DARTS_AVAILABLE

        if DARTS_AVAILABLE:
            print("✓ Darts is available and imported successfully")
        else:
            print("✓ Darts is not available, but handled gracefully")
        return True
    except ImportError as e:
        print(f"✗ Darts decorator import failed: {e}")
        return False


def test_darts_functionality_when_missing():
    """Test that darts functionality raises appropriate errors when missing."""
    try:
        from tsdb.decorators.darts_decorator import timeseries_storage, DARTS_AVAILABLE

        if not DARTS_AVAILABLE:
            print("✓ Darts not available - this is expected for testing")

            # Test that the decorator can still be imported
            @timeseries_storage(table_name="test_table")
            class TestManager:
                pass

            print("✓ Darts decorator can be applied even without darts installed")

            # Test that methods raise appropriate errors
            try:
                TestManager.save_timeseries(None, "test")
                print("✗ Should have raised ImportError")
                return False
            except ImportError as e:
                if "uv add tsdb[forecast]" in str(e):
                    print("✓ Appropriate error message with uv command shown")
                    return True
                else:
                    print(f"✗ Error message doesn't contain uv command: {e}")
                    return False
        else:
            print("✓ Darts is available - functionality should work normally")
            return True

    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False


def main():
    """Run all tests."""
    print("Testing optional darts dependency configuration...")
    print("=" * 50)

    tests = [
        test_basic_imports,
        test_darts_optional_import,
        test_darts_functionality_when_missing,
    ]

    results = []
    for test in tests:
        print(f"\nRunning {test.__name__}:")
        results.append(test())

    print("\n" + "=" * 50)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("✓ All tests passed! Darts is properly configured as optional.")
    else:
        print("✗ Some tests failed. Check the configuration.")


if __name__ == "__main__":
    main()
