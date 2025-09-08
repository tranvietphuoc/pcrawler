-- SQLite schema cho complete crawling workflow

-- Bảng lưu HTML content của detail pages
CREATE TABLE IF NOT EXISTS detail_html_storage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    company_url TEXT NOT NULL,
    industry TEXT,
    html_content TEXT NOT NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending', -- pending, processed, failed
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu thông tin chi tiết company được extract từ detail page
CREATE TABLE IF NOT EXISTS company_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detail_html_id INTEGER NOT NULL,
    company_name TEXT NOT NULL,
    company_url TEXT NOT NULL,
    address TEXT,
    phone TEXT,
    website TEXT,
    facebook TEXT,
    linkedin TEXT,
    tiktok TEXT,
    youtube TEXT,
    instagram TEXT,
    description TEXT,
    created_year TEXT,
    revenue TEXT,
    scale TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (detail_html_id) REFERENCES detail_html_storage (id)
);

-- Bảng lưu HTML content của website/facebook pages
CREATE TABLE IF NOT EXISTS contact_html_storage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    url TEXT NOT NULL,
    url_type TEXT NOT NULL, -- website, facebook, google
    html_content TEXT NOT NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending', -- pending, processed, failed
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu kết quả email extraction
CREATE TABLE IF NOT EXISTS email_extraction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_html_id INTEGER NOT NULL,
    company_name TEXT NOT NULL,
    extracted_emails TEXT, -- JSON array of emails
    email_source TEXT, -- website, facebook, google
    extraction_method TEXT, -- regex, crawl4ai, etc
    confidence_score REAL, -- 0.0 to 1.0
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contact_html_id) REFERENCES contact_html_storage (id)
);

-- Bảng final result kết hợp tất cả thông tin
CREATE TABLE IF NOT EXISTS final_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    company_url TEXT,
    address TEXT,
    phone TEXT,
    website TEXT,
    facebook TEXT,
    linkedin TEXT,
    tiktok TEXT,
    youtube TEXT,
    instagram TEXT,
    industry TEXT,
    description TEXT,
    created_year TEXT,
    revenue TEXT,
    scale TEXT,
    extracted_emails TEXT, -- Single email per row
    email_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_detail_html_status ON detail_html_storage(status);
CREATE INDEX IF NOT EXISTS idx_detail_html_company ON detail_html_storage(company_name);
CREATE INDEX IF NOT EXISTS idx_company_details_name ON company_details(company_name);
CREATE INDEX IF NOT EXISTS idx_contact_html_status ON contact_html_storage(status);
CREATE INDEX IF NOT EXISTS idx_contact_html_company ON contact_html_storage(company_name);
CREATE INDEX IF NOT EXISTS idx_contact_html_type ON contact_html_storage(url_type);
CREATE INDEX IF NOT EXISTS idx_email_extraction_company ON email_extraction(company_name);
CREATE INDEX IF NOT EXISTS idx_email_extraction_html_id ON email_extraction(contact_html_id);
CREATE INDEX IF NOT EXISTS idx_final_results_company ON final_results(company_name);
