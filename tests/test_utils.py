from unittest import TestCase

from tecton_client._internal.utils import validate_url


class TestValidation(TestCase):
    def test_validate_url(self):
        validate_url("https://explore.tecton.ai")
        validate_url("http://explore.tecton.ai")
        validate_url("http://127.0.0.1")
        with self.assertRaisesRegexp(ValueError, "Invalid URL: Must begin with"):
            validate_url("explore.tecton.ai")

        with self.assertRaisesRegexp(ValueError, "Invalid URL: Must begin with"):
            validate_url("https.explore.tecton.ai")

        with self.assertRaisesRegexp(ValueError, "Invalid URL: Must begin with"):
            validate_url("ftp://explore.tecton.ai")
