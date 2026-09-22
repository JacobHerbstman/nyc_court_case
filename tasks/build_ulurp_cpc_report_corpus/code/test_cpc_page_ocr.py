"""Regression checks for scanned attachments following a CPC resolution."""
import unittest
from unittest.mock import patch
from pathlib import Path

import build_ulurp_cpc_report_corpus as corpus


class AttachmentOCR(unittest.TestCase):
    def test_embedded_resolution_does_not_end_ocr(self):
        main = 'RESOLVED BY THE CITY PLANNING COMMISSION\n' + 'main ' * 60
        attachment = 'Community Board recommendation: oppose this application. ' * 12
        with patch.object(corpus, 'pdf_page_count', return_value=3), patch.object(
            corpus, 'ocr_pdf_page', side_effect=[attachment, '']
        ) as ocr:
            text, repaired, skipped, short, boundary = corpus.add_missing_report_page_ocr(
                Path('fixture.pdf'), main + '\f\f', 200, 90, 50)
        self.assertEqual([c.args[1] for c in ocr.call_args_list], [2, 3])
        self.assertIn(attachment, text.split('\f')[1])
        self.assertEqual((repaired, skipped, short, boundary), ([2], [], [3], 1))

    def test_resolution_found_by_ocr_keeps_following_attachments(self):
        main = 'RESOLVED BY THE CITY PLANNING COMMISSION\n' + 'main ' * 60
        attachment = 'Council Member requests an affordability condition. ' * 12
        with patch.object(corpus, 'pdf_page_count', return_value=2), patch.object(
            corpus, 'ocr_pdf_page', side_effect=[main, attachment]
        ) as ocr:
            text, repaired, skipped, short, boundary = corpus.add_missing_report_page_ocr(
                Path('fixture.pdf'), '\f', 200, 90, 50)
        self.assertEqual(ocr.call_count, 2)
        self.assertIn(attachment, text)
        self.assertEqual((repaired, skipped, short, boundary), ([1, 2], [], [], 1))

    def test_attachment_timeout_remains_explicit(self):
        main = 'RESOLVED BY THE CITY PLANNING COMMISSION\n' + 'main ' * 60
        with patch.object(corpus, 'pdf_page_count', return_value=2), patch.object(
            corpus, 'ocr_pdf_page', return_value=None
        ):
            text, repaired, skipped, short, boundary = corpus.add_missing_report_page_ocr(
                Path('fixture.pdf'), main + '\f', 200, 90, 50)
        self.assertEqual((repaired, skipped, short, boundary), ([], [2], [2], 1))
        self.assertEqual(text.split('\f')[0], main)


if __name__ == '__main__':
    unittest.main()
