"""Regression checks for links emitted by MkDocs under different site URLs."""
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import check_docs


class SiteURLTests(unittest.TestCase):
    def check_site(self, base_path, extra_link="", canonical=True):
        with tempfile.TemporaryDirectory() as directory:
            site = Path(directory)
            home = f'<link rel="canonical" href="https://docs.example{base_path}">'
            (site / "index.html").write_text(home if canonical else "<h1>Home</h1>")
            (site / "assets").mkdir()
            (site / "assets/theme.css").write_text("body { color: black; }")
            (site / "guide").mkdir()
            (site / "guide/index.html").write_text(
                '<h1 id="detail">Guide</h1><a href="../">Home</a>'
            )
            # Error pages use absolute paths because the requested URL is unknown.
            (site / "404.html").write_text(
                f'<link href="{base_path}assets/theme.css" rel="stylesheet">'
                f'<a href="{base_path}.">Home</a>'
                f'<a href="{base_path}guide/#detail">Guide</a>'
                f'<a href="{extra_link}">Extra</a>'
            )
            with patch.object(check_docs, "SITE", site), redirect_stdout(io.StringIO()):
                check_docs.main()

    def test_root_and_subpath_hosting(self):
        for base in ("/", "/amends/", "/docs/reference/"):
            with self.subTest(base=base):
                self.check_site(base)

    def test_subpath_rejects_links_outside_project(self):
        for link in ("/assets/theme.css", "/amends-other/guide/"):
            with self.subTest(link=link), self.assertRaisesRegex(SystemExit, "outside project URL"):
                self.check_site("/amends/", link)

    def test_broken_links_still_fail(self):
        for base in ("/", "/amends/"):
            for suffix, error in (("missing.css", "missing local target"),
                                  ("guide/#absent", "missing anchor"),
                                  ("../index.html", "missing local target")):
                with self.subTest(base=base, suffix=suffix), self.assertRaisesRegex(SystemExit, error):
                    self.check_site(base, base + suffix)

    def test_missing_canonical_fails(self):
        with self.assertRaisesRegex(SystemExit, "missing its canonical site URL"):
            self.check_site("/", canonical=False)


if __name__ == "__main__":
    unittest.main()
