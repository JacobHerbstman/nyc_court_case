../../../data_raw/dcp_zap_full_export/20260914/zap_project_data.csv: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 180 'https://data.cityofnewyork.us/api/views/hgx4-8ukb/rows.csv?accessType=DOWNLOAD' -o ../temp/zap_project_data.csv
	python3 -c 'import csv; r=list(csv.DictReader(open("../temp/zap_project_data.csv"))); assert r and "project_id" in r[0] and "project_status" in r[0]'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_project_data.csv", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_project_data.csv"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_project_data.csv $@

../../../data_raw/dcp_zap_full_export/20260914/zap_project_metadata.json: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 60 'https://data.cityofnewyork.us/api/views/hgx4-8ukb.json' -o ../temp/zap_project_metadata.json
	python3 -c 'import json; assert json.load(open("../temp/zap_project_metadata.json"))["id"] == "hgx4-8ukb"'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_project_metadata.json", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_project_metadata.json"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_project_metadata.json $@

../../../data_raw/dcp_zap_full_export/20260914/zap_project_count.json: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 60 'https://data.cityofnewyork.us/resource/hgx4-8ukb.json?%24select=count(*)' -o ../temp/zap_project_count.json
	python3 -c 'import json; assert int(json.load(open("../temp/zap_project_count.json"))[0]["count"]) > 0'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_project_count.json", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_project_count.json"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_project_count.json $@

../../../data_raw/dcp_zap_full_export/20260914/zap_bbl.csv: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 180 'https://data.cityofnewyork.us/api/views/2iga-a6mk/rows.csv?accessType=DOWNLOAD' -o ../temp/zap_bbl.csv
	python3 -c 'import csv; r=list(csv.DictReader(open("../temp/zap_bbl.csv"))); assert r and "project_id" in r[0] and "bbl" in r[0]'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_bbl.csv", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_bbl.csv"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_bbl.csv $@

../../../data_raw/dcp_zap_full_export/20260914/zap_bbl_metadata.json: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 60 'https://data.cityofnewyork.us/api/views/2iga-a6mk.json' -o ../temp/zap_bbl_metadata.json
	python3 -c 'import json; assert json.load(open("../temp/zap_bbl_metadata.json"))["id"] == "2iga-a6mk"'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_bbl_metadata.json", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_bbl_metadata.json"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_bbl_metadata.json $@

../../../data_raw/dcp_zap_full_export/20260914/zap_bbl_count.json: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 60 'https://data.cityofnewyork.us/resource/2iga-a6mk.json?%24select=count(*)' -o ../temp/zap_bbl_count.json
	python3 -c 'import json; assert int(json.load(open("../temp/zap_bbl_count.json"))[0]["count"]) > 0'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zap_bbl_count.json", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zap_bbl_count.json"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zap_bbl_count.json $@

../../../data_raw/dcp_zap_full_export/20260914/zapprojects_datadictionary.xlsx: sources.make source_snapshot_sha256.json | ../temp
	mkdir -p $(@D)
	curl -fLsS --max-time 60 'https://data.cityofnewyork.us/api/views/hgx4-8ukb/files/b118172c-e5f1-4a46-a53f-f403086b1043?download=true&filename=zapprojects_datadictionary.xlsx' -o ../temp/zapprojects_datadictionary.xlsx
	python3 -c 'import zipfile; assert zipfile.is_zipfile("../temp/zapprojects_datadictionary.xlsx")'
	python3 -c 'import hashlib,json; assert hashlib.sha256(open("../temp/zapprojects_datadictionary.xlsx", "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))["zapprojects_datadictionary.xlsx"], "Source changed: preserve the recorded snapshot and use a new vintage"'
	mv ../temp/zapprojects_datadictionary.xlsx $@
