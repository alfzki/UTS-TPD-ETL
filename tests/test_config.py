import sys
import os
import unittest

# Add the root directory to sys.path to import etl modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.config import SUBJECT_MAPPING, LEVEL_MAPPING, PAYMENT_METHOD_CATEGORY

class TestConfigMappings(unittest.TestCase):
    def test_subject_mapping(self):
        # Existing ones
        self.assertEqual(SUBJECT_MAPPING["Mathematics"], "Matematika")
        # New ones from task
        self.assertEqual(SUBJECT_MAPPING["Mathematics (Saintek)"], "Matematika")
        self.assertEqual(SUBJECT_MAPPING["English (General)"], "Bahasa Inggris")

    def test_level_mapping(self):
        # Existing ones
        self.assertEqual(LEVEL_MAPPING["beginner"], "dasar")
        # Ensure all requested ones are there
        self.assertEqual(LEVEL_MAPPING["easy"], "dasar")
        self.assertEqual(LEVEL_MAPPING["medium"], "menengah")
        self.assertEqual(LEVEL_MAPPING["hard"], "lanjut")
        self.assertEqual(LEVEL_MAPPING["pemula"], "dasar")
        self.assertEqual(LEVEL_MAPPING["sedang"], "menengah")
        self.assertEqual(LEVEL_MAPPING["mahir"], "lanjut")

    def test_payment_method_category(self):
        # Check for new category values requested in task
        # Task says: "e-wallet": "pembayaran digital"
        # Current config has "pembayaran_digital" (with underscore)
        # Task wants "pembayaran digital" (with space)
        
        self.assertEqual(PAYMENT_METHOD_CATEGORY["e-wallet"], "pembayaran digital")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["QRIS"], "pembayaran digital")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["GoPay"], "pembayaran digital")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["OVO"], "pembayaran digital")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["ShopeePay"], "pembayaran digital")
        
        self.assertEqual(PAYMENT_METHOD_CATEGORY["transfer_bank"], "transfer")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["bank_transfer"], "transfer")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["virtual_account"], "transfer")
        
        self.assertEqual(PAYMENT_METHOD_CATEGORY["kartu_kredit"], "kartu")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["credit_card"], "kartu")
        
        self.assertEqual(PAYMENT_METHOD_CATEGORY["minimarket"], "retail")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["alfamart"], "retail")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["indomaret"], "retail")
        
        self.assertEqual(PAYMENT_METHOD_CATEGORY["gratis"], "gratis")
        self.assertEqual(PAYMENT_METHOD_CATEGORY["free"], "gratis")

if __name__ == '__main__':
    unittest.main()
