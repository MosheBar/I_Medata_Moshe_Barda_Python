-- Create schema for medical data
CREATE SCHEMA IF NOT EXISTS medical;

-- Patient Information Table
CREATE TABLE IF NOT EXISTS medical.patients (
    patient_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    primary_physician VARCHAR(100),
    insurance_provider VARCHAR(100) NOT NULL,
    blood_type VARCHAR(3) NOT NULL,
    allergies TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Lab Tests Table
CREATE TABLE IF NOT EXISTS medical.lab_tests (
    test_id VARCHAR(10) PRIMARY KEY,
    patient_id VARCHAR(10) REFERENCES medical.patients(patient_id),
    test_name VARCHAR(100) NOT NULL,
    order_date DATE NOT NULL,
    order_time TIME NOT NULL,
    ordering_physician VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Lab Results Table
CREATE TABLE IF NOT EXISTS medical.lab_results (
    result_id SERIAL PRIMARY KEY,
    test_id VARCHAR(10) REFERENCES medical.lab_tests(test_id),
    result_value VARCHAR(100) NOT NULL,
    unit VARCHAR(50),
    reference_range VARCHAR(100),
    result_status VARCHAR(20) NOT NULL,
    result_date TIMESTAMP WITH TIME ZONE NOT NULL,
    analyzed_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Admissions Table
CREATE TABLE IF NOT EXISTS medical.admissions (
    admission_id VARCHAR(10) PRIMARY KEY,
    patient_id VARCHAR(10) REFERENCES medical.patients(patient_id),
    admission_date TIMESTAMP WITH TIME ZONE NOT NULL,
    discharge_date TIMESTAMP WITH TIME ZONE,
    admission_type VARCHAR(50) NOT NULL,
    admitting_physician VARCHAR(100) NOT NULL,
    diagnosis TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_lab_tests_patient_id ON medical.lab_tests(patient_id);
CREATE INDEX idx_lab_results_test_id ON medical.lab_results(test_id);
CREATE INDEX idx_admissions_patient_id ON medical.admissions(patient_id);
CREATE INDEX idx_patients_name ON medical.patients(last_name, first_name);

-- Create trigger function for updating timestamps
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for all tables
CREATE TRIGGER update_patients_timestamp
    BEFORE UPDATE ON medical.patients
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_lab_tests_timestamp
    BEFORE UPDATE ON medical.lab_tests
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_lab_results_timestamp
    BEFORE UPDATE ON medical.lab_results
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_admissions_timestamp
    BEFORE UPDATE ON medical.admissions
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp(); 