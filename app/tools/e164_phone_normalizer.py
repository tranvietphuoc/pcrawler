import asyncio
import re
import pandas as pd
import argparse
import logging
from typing import List, Optional, Dict
import phonenumbers
from phonenumbers import NumberParseException

from app.crawler.async_context_manager import get_context_manager
from config import CrawlerConfig

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def normalize_to_e164(phone_text: str, default_country: str = "VN") -> Optional[str]:
    """Chuẩn hóa số điện thoại theo định dạng E164"""
    if not phone_text:
        return None
    
    # Convert number to text if needed (Excel often stores numbers as number type)
    if isinstance(phone_text, (int, float)):
        phone_text = str(int(phone_text))  # Convert to string, remove decimal if any
    
    # Loại bỏ khoảng trắng và ký tự đặc biệt
    cleaned = re.sub(r'[^\d+]', '', str(phone_text).strip())
    
    if not cleaned:
        return None
    
    # Xử lý số có độ dài == 9 (thường là số VN thiếu +84)
    if len(cleaned) == 9:
        # Thêm +84 vào đầu
        cleaned = '+84' + cleaned
    
    # Xử lý số có độ dài > 10 (giữ nguyên)
    if len(cleaned) > 10:
        # Nếu không có +, thử thêm + và parse
        if not cleaned.startswith('+'):
            try:
                parsed = phonenumbers.parse('+' + cleaned, default_country)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
            # Nếu không parse được, thêm + và giữ nguyên
            return '+' + cleaned
        # Nếu đã có +, parse trực tiếp
        else:
            try:
                parsed = phonenumbers.parse(cleaned, default_country)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
            # Nếu không parse được, giữ nguyên
            return cleaned
    
    try:
        # Parse số điện thoại
        parsed = phonenumbers.parse(cleaned, default_country)
        
        # Kiểm tra tính hợp lệ
        if phonenumbers.is_valid_number(parsed):
            # Trả về định dạng E164
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        else:
            return None
            
    except NumberParseException:
        # Nếu không parse được, thử các cách khác
        return fallback_normalize(cleaned, default_country)


def fallback_normalize(phone_text: str, default_country: str = "VN") -> Optional[str]:
    """Fallback normalization cho các trường hợp đặc biệt"""
    if not phone_text:
        return None
    
    # Loại bỏ tất cả ký tự không phải số và dấu +
    cleaned = re.sub(r'[^\d+]', '', phone_text)
    
    if not cleaned:
        return None
    
    # Xử lý các trường hợp đặc biệt cho số Việt Nam
    if default_country == "VN":
        # Danh sách mã vùng Việt Nam
        vn_area_codes = [
            "032", "033", "034", "035", "036", "037", "038", "039",  # Mobile
            "052", "055", "056", "058", "059",  # Central
            "070", "076", "077", "078", "079",  # Mobile
            "081", "082", "083", "084", "085", "086", "087", "088", "089",  # Mobile
            "090", "091", "092", "093", "094", "096", "097", "098", "099",  # Mobile
            "02"  # Hà Nội
        ]
        
        vn_area_codes_no_zero = [code[1:] for code in vn_area_codes]  # Bỏ số 0 đầu
        
        def normalize_phone_number(phone: str) -> Optional[str]:
            """Chuẩn hóa số điện thoại Việt Nam"""
            if not phone:
                return None
            
            # Loại bỏ tất cả ký tự không phải số và dấu +
            cleaned = re.sub(r'[^\d+]', '', phone)
            
            if not cleaned:
                return None
            
            # Nếu đã có +84, giữ nguyên
            if cleaned.startswith('+84'):
                try:
                    parsed = phonenumbers.parse(cleaned, "VN")
                    if phonenumbers.is_valid_number(parsed):
                        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                except NumberParseException:
                    pass
            
            # Nếu bắt đầu bằng 84 (không có +)
            elif cleaned.startswith('84'):
                # Kiểm tra mã vùng
                if len(cleaned) >= 10:
                    area_code = cleaned[2:5]  # Lấy 3 số sau 84
                    if area_code in vn_area_codes_no_zero:
                        try:
                            parsed = phonenumbers.parse('+' + cleaned, "VN")
                            if phonenumbers.is_valid_number(parsed):
                                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                        except NumberParseException:
                            pass
            
            # Nếu bắt đầu bằng 0
            elif cleaned.startswith('0'):
                if len(cleaned) >= 10:
                    # Kiểm tra mã vùng 02 (Hà Nội) - cần 8 số sau 02
                    if cleaned.startswith('02') and len(cleaned) == 11:
                        try:
                            parsed = phonenumbers.parse('+84' + cleaned[1:], "VN")
                            if phonenumbers.is_valid_number(parsed):
                                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                        except NumberParseException:
                            pass
                    # Các mã vùng khác - cần 7 số sau mã vùng
                    elif len(cleaned) == 10:
                        area_code = cleaned[:3]
                        if area_code in vn_area_codes:
                            try:
                                parsed = phonenumbers.parse('+84' + cleaned[1:], "VN")
                                if phonenumbers.is_valid_number(parsed):
                                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                            except NumberParseException:
                                pass
            
            return None
        
        return normalize_phone_number(cleaned)
    
    try:
        parsed = phonenumbers.parse(cleaned, default_country)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except NumberParseException:
        pass
    
    return None


def split_phone_numbers(phone_text: str) -> List[str]:
    """Tách các số điện thoại từ text, trả về danh sách các số đã chuẩn hóa"""
    if not phone_text:
        return []
    
    # Convert number to text if needed (Excel often stores numbers as number type)
    if isinstance(phone_text, (int, float)):
        phone_text = str(int(phone_text))  # Convert to string, remove decimal if any
    
    # Bước 1: Tách theo ký tự phân tách chính (ưu tiên ;, /)
    primary_separators = [';', '/', ',', '\n', '|']
    phones = [phone_text]
    for sep in primary_separators:
        new_phones = []
        for phone in phones:
            new_phones.extend([p.strip() for p in phone.split(sep) if p.strip()])
        phones = new_phones
    
    # Bước 2: Tách theo ký tự phân tách phụ (ưu tiên cao)
    secondary_separators = ['\t', '-', '.']
    for sep in secondary_separators:
        new_phones = []
        for phone in phones:
            new_phones.extend([p.strip() for p in phone.split(sep) if p.strip()])
        phones = new_phones
    
    # Bước 3: Tách theo space và \n (ưu tiên thấp nhất)
    final_separators = [' ']
    for sep in final_separators:
        new_phones = []
        for phone in phones:
            new_phones.extend([p.strip() for p in phone.split(sep) if p.strip()])
        phones = new_phones
    
    # Bước 4: Remove space trong từng số điện thoại
    cleaned_phones = []
    for phone in phones:
        # Remove space trong số điện thoại
        cleaned_phone = re.sub(r'\s+', '', phone)
        if cleaned_phone:
            cleaned_phones.append(cleaned_phone)
    
    # Bước 5: Chuẩn hóa từng số
    normalized_phones = []
    for phone in cleaned_phones:
        normalized = normalize_phone_with_validation(phone)
        if normalized:
            normalized_phones.append(normalized)
    
    return normalized_phones


def normalize_phone_with_validation(phone: str) -> Optional[str]:
    """
    Chuẩn hóa số điện thoại với validation cải tiến:
    - Xử lý số có độ dài == 9 (thêm +84)
    - Giữ lại số quốc tế (thường dài hơn)
    - Chỉ validate độ dài cho số Việt Nam
    - Xử lý số quốc tế dài hơn 10 ký tự
    """
    if not phone:
        return None
    
    # Convert number to text if needed (Excel often stores numbers as number type)
    if isinstance(phone, (int, float)):
        phone = str(int(phone))  # Convert to string, remove decimal if any
    
    # Loại bỏ khoảng trắng và ký tự đặc biệt
    cleaned = re.sub(r'[^\d+]', '', str(phone).strip())
    
    if not cleaned:
        return None
    
    # Xử lý số có độ dài == 9 (thường là số VN thiếu +84)
    if len(cleaned) == 9:
        # Thêm +84 vào đầu
        cleaned = '+84' + cleaned
    
    # Xử lý số có độ dài > 10 (giữ nguyên)
    if len(cleaned) > 10:
        # Nếu không có +, thử thêm + và parse
        if not cleaned.startswith('+'):
            try:
                parsed = phonenumbers.parse('+' + cleaned, None)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
            # Nếu không parse được, thêm + và giữ nguyên
            return '+' + cleaned
        # Nếu đã có +, parse trực tiếp
        else:
            try:
                parsed = phonenumbers.parse(cleaned, None)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
            # Nếu không parse được, giữ nguyên
            return cleaned
    
    # Nếu đã có +84 (số Việt Nam)
    if cleaned.startswith('+84'):
        try:
            parsed = phonenumbers.parse(cleaned, "VN")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except NumberParseException:
            pass
    
    # Nếu bắt đầu bằng 84 (không có +)
    elif cleaned.startswith('84'):
        if len(cleaned) >= 10:
            try:
                parsed = phonenumbers.parse('+' + cleaned, "VN")
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
    
    # Nếu bắt đầu bằng 0 (số Việt Nam)
    elif cleaned.startswith('0'):
        if len(cleaned) >= 10:
            try:
                parsed = phonenumbers.parse('+84' + cleaned[1:], "VN")
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
    
    # Số quốc tế khác (giữ nguyên nếu hợp lệ)
    elif cleaned.startswith('+'):
        try:
            # Thử parse với country code mặc định
            parsed = phonenumbers.parse(cleaned, None)
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except NumberParseException:
            pass
    
    # Fallback: thử normalize với logic cũ
    return fallback_normalize(cleaned, "VN")


def split_multiple_phones_to_rows(df: pd.DataFrame, phone_column: str) -> pd.DataFrame:
    """
    Tách các dòng có nhiều số điện thoại thành nhiều dòng riêng biệt
    Mỗi dòng chỉ chứa 1 số điện thoại
    """
    logger.info(f"=== TÁCH CÁC DÒNG CÓ NHIỀU SỐ ĐIỆN THOẠI ===")
    
    new_rows = []
    split_count = 0
    
    for idx, row in df.iterrows():
        phone_text = str(row[phone_column]) if pd.notna(row[phone_column]) else ""
        
        if phone_text and phone_text.strip():
            # Tách các số điện thoại
            phones = split_phone_numbers(phone_text)
            
            if len(phones) > 1:
                # Nếu có nhiều số, tạo nhiều dòng
                split_count += len(phones) - 1
                for phone in phones:
                    new_row = row.copy()
                    new_row[phone_column] = phone
                    new_rows.append(new_row)
            elif len(phones) == 1:
                # Nếu chỉ có 1 số, giữ nguyên
                new_row = row.copy()
                new_row[phone_column] = phones[0]
                new_rows.append(new_row)
            else:
                # Nếu không có số hợp lệ, giữ nguyên dòng với giá trị rỗng
                new_row = row.copy()
                new_row[phone_column] = ""
                new_rows.append(new_row)
        else:
            # Nếu không có text, giữ nguyên dòng
            new_row = row.copy()
            new_row[phone_column] = ""
            new_rows.append(new_row)
    
    result_df = pd.DataFrame(new_rows)
    logger.info(f"Đã tách {split_count} dòng thành {len(result_df)} dòng")
    
    return result_df


def preprocess_excel_data(df: pd.DataFrame, phone_column: str = None) -> pd.DataFrame:
    """
    Bước 1: Preprocess - tách số điện thoại và tạo cột phone
    Mỗi dòng chỉ chứa 1 số điện thoại đã chuẩn hóa
    """
    logger.info("=== BƯỚC 1: PREPROCESS - TÁCH SỐ ĐIỆN THOẠI ===")
    
    # Tìm cột chứa số điện thoại
    if phone_column is None:
        # Tìm cột đầu tiên có chứa số điện thoại
        for col in df.columns:
            sample_values = df[col].dropna().head(10)
            # Kiểm tra cả string và number
            if any(re.search(r'\d{9,}', str(val)) for val in sample_values):
                phone_column = col
                break
    
    if phone_column is None:
        raise ValueError("Không tìm thấy cột chứa số điện thoại")
    
    logger.info(f"Sử dụng cột: {phone_column}")
    
    # Tạo cột phone từ cột gốc - xử lý cả number và text
    def convert_to_text(value):
        if pd.isna(value):
            return ""
        # Convert number to text if needed
        if isinstance(value, (int, float)):
            return str(int(value))  # Remove decimal if any
        return str(value)
    
    df['phone'] = df[phone_column].apply(convert_to_text)
    
    # Tách các dòng có nhiều số điện thoại thành nhiều dòng
    result_df = split_multiple_phones_to_rows(df, 'phone')
    
    logger.info(f"Đã tách thành {len(result_df)} dòng (từ {len(df)} dòng gốc)")
    
    return result_df


async def extract_phones_with_crawl4ai(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """
    Bước 2: Crawl4AI Extract - extract thêm số điện thoại từ text gốc
    """
    logger.info("=== BƯỚC 2: CRAWL4AI EXTRACT ===")
    
    # Tìm cột gốc chứa text
    original_column = None
    for col in df.columns:
        if col != 'phone' and df[col].dtype == 'object':
            original_column = col
            break
    
    if original_column is None:
        logger.warning("Không tìm thấy cột gốc để extract")
        return df
    
    # Query để extract số điện thoại
    phone_query = """
    Extract ALL phone numbers from the text. 
    For Vietnamese numbers:
    - Starting with 0, 84, or +84: convert to +84 E164 format
    - Valid area codes: 032, 033, 034, 035, 036, 037, 038, 039, 052, 055, 056, 058, 059, 070, 076, 077, 078, 079, 081, 082, 083, 084, 085, 086, 087, 088, 089, 090, 091, 092, 093, 094, 096, 097, 098, 099, 02
    - 02 area code needs 8 digits after it, others need 7 digits
    For international numbers:
    - Starting with +: keep as is if valid E164 format
    - Longer than 10 digits (not starting with 01): try to format as E164 with common country codes (82, 86, 1, 44, 33, 49, 81, 65, 60, 66)
    - Do not convert international numbers to Vietnamese format
    Remove spaces, dashes, parentheses.
    Separate multiple numbers with semicolon (;).
    """
    
    context_manager = get_context_manager()
    crawler_id = f"phone_extractor_{asyncio.current_task().get_name()}"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    viewport = {"width": 1920, "height": 1080}
    
    async def process_single_row(row_data):
        idx, row = row_data
        phone_text = str(row[original_column]) if pd.notna(row[original_column]) else ""
        
        if not phone_text or phone_text.strip() == "":
            return idx, ""
        
        try:
            # Dùng Crawl4AI để extract
            async with context_manager.get_crawl4ai_crawler(crawler_id, user_agent, viewport) as crawler:
                result = await crawler.arun(
                    url="raw:" + phone_text,
                    word_count_threshold=1,
                    extraction_strategy=phone_query,
                    bypass_cache=False,
                    wait_for="domcontentloaded",
                    delay_before_return_html=0.1
                )
                
                if result and result.extracted_content:
                    # Extract và normalize phones theo E164
                    extracted_phones = []
                    for line in result.extracted_content.split('\n'):
                        line = line.strip()
                        if line:
                            normalized = normalize_phone_with_validation(line)
                            if normalized:
                                extracted_phones.append(normalized)
                    
                    return idx, "; ".join(extracted_phones)
                else:
                    return idx, ""
                    
        except Exception as e:
            logger.warning(f"Failed to process row {idx}: {e}")
            return idx, ""
    
    # Xử lý theo batch
    semaphore = asyncio.Semaphore(10)  # Concurrent processing
    
    async def process_with_semaphore(row_data):
        async with semaphore:
            return await process_single_row(row_data)
    
    # Tạo cột extracted_phone
    df['extracted_phone'] = ""
    
    for i in range(0, len(df), batch_size):
        batch_end = min(i + batch_size, len(df))
        batch_df = df.iloc[i:batch_end]
        
        # Tạo tasks cho batch hiện tại
        tasks = [process_with_semaphore((idx, row)) for idx, row in batch_df.iterrows()]
        
        # Chạy song song tất cả tasks trong batch
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cập nhật cột extracted_phone
        for result in batch_results:
            if isinstance(result, Exception):
                logger.warning(f"Batch processing error: {result}")
                continue
            
            idx, extracted_phones = result
            df.at[idx, 'extracted_phone'] = extracted_phones
        
        logger.info(f"Processed batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1}")
    
    logger.info("Hoàn thành Crawl4AI extraction")
    return df


def create_final_phone_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bước 3: Tạo cột final_phone và split thành nhiều dòng
    """
    logger.info("=== BƯỚC 3: TẠO FINAL_PHONE VÀ SPLIT ===")
    
    def is_valid_phone(phone: str) -> bool:
        """
        Kiểm tra tính hợp lệ của số điện thoại:
        - Số Việt Nam: validate độ dài theo quy tắc VN
        - Số quốc tế: giữ nguyên nếu có format E164 hợp lệ
        """
        if not phone:
            return False
        
        # Số Việt Nam (+84)
        if phone.startswith('+84'):
            # Bỏ +84, lấy phần còn lại
            number_part = phone[3:]
            
            # Kiểm tra độ dài cho số VN
            if len(number_part) == 9:  # 02 area code (8 digits + 1)
                return number_part.startswith('2')
            elif len(number_part) == 10:  # Other area codes (7 digits + 3)
                return True
            
            return False
        
        # Số quốc tế khác - giữ nguyên nếu có format E164 hợp lệ
        else:
            try:
                parsed = phonenumbers.parse(phone, None)
                return phonenumbers.is_valid_number(parsed)
            except NumberParseException:
                return False
        
        return False
    
    # Tạo cột final_phone_source để chứa nguồn số điện thoại
    df['final_phone_source'] = ""
    
    for idx, row in df.iterrows():
        # Ưu tiên cột phone nếu có, nếu không thì dùng extracted_phone
        primary_phone = row['phone'] if pd.notna(row['phone']) and str(row['phone']).strip() else ""
        extracted_phone = row['extracted_phone'] if pd.notna(row['extracted_phone']) and str(row['extracted_phone']).strip() else ""
        
        # Chọn nguồn số điện thoại chính
        if primary_phone:
            phone_source = primary_phone
        elif extracted_phone:
            phone_source = extracted_phone
        else:
            phone_source = ""
        
        df.at[idx, 'final_phone_source'] = phone_source
    
    # Tách các dòng có nhiều số điện thoại trong final_phone_source
    result_df = split_multiple_phones_to_rows(df, 'final_phone_source')
    
    # Tạo cột final_phone từ final_phone_source đã được split
    result_df['final_phone'] = result_df['final_phone_source'].apply(
        lambda x: x if is_valid_phone(x) else ""
    )
    
    # Xóa cột tạm
    result_df = result_df.drop('final_phone_source', axis=1)
    
    logger.info(f"Đã tạo {len(result_df)} dòng với cột final_phone")
    
    return result_df


async def process_excel_e164(input_file: str, output_file: str = None, phone_column: str = None):
    """
    Xử lý file Excel theo 3 bước:
    1. Preprocess: tách số điện thoại và tạo cột phone (split các dòng có nhiều số)
    2. Crawl4AI Extract: extract thêm số từ text gốc
    3. Create Final Phone: tạo cột final_phone và split lại thành nhiều dòng
    """
    logger.info(f"Bắt đầu xử lý file: {input_file}")
    
    # Đọc file Excel
    try:
        df = pd.read_excel(input_file)
        logger.info(f"Đã đọc {len(df)} dòng từ file Excel")
    except Exception as e:
        logger.error(f"Lỗi đọc file Excel: {e}")
        return
    
    # Bước 1: Preprocess
    df = preprocess_excel_data(df, phone_column)
    
    # Bước 2: Crawl4AI Extract
    df = await extract_phones_with_crawl4ai(df)
    
    # Bước 3: Create Final Phone
    df = create_final_phone_column(df)
    
    # Lưu kết quả
    if output_file is None:
        output_file = input_file.replace('.xlsx', '_e164_processed.xlsx')
    
    try:
        df.to_excel(output_file, index=False)
        logger.info(f"Đã lưu kết quả vào: {output_file}")
        logger.info(f"Tổng số dòng kết quả: {len(df)}")
        
        # Thống kê
        valid_phones = df[df['final_phone'].notna() & (df['final_phone'] != '')]
        logger.info(f"Số dòng có số điện thoại hợp lệ: {len(valid_phones)}")
        
    except Exception as e:
        logger.error(f"Lỗi lưu file: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Chuẩn hóa số điện thoại theo E164")
    parser.add_argument("input_file", help="Đường dẫn file Excel đầu vào")
    parser.add_argument("-o", "--output", help="Đường dẫn file Excel đầu ra")
    parser.add_argument("-c", "--column", help="Tên cột chứa số điện thoại")
    parser.add_argument("-b", "--batch-size", type=int, default=50, help="Kích thước batch cho Crawl4AI")
    
    args = parser.parse_args()
    
    await process_excel_e164(
        input_file=args.input_file,
        output_file=args.output,
        phone_column=args.column
    )


if __name__ == "__main__":
    asyncio.run(main())