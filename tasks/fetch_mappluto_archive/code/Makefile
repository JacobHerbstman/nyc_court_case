PLUTO_VINTAGES := 03c 04c 05d 06c 07c 09v1 10v1 11v1 12v1 13v1 14v1 15v1 16v1 17v1
SHA_CHECK = python3 -c 'import hashlib,json,sys; assert hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest() == json.load(open("source_snapshot_sha256.json"))[sys.argv[2]], "Source changed: keep the recorded file and add a new vintage"'

all: ../output/mappluto_files.csv \
	$(foreach v,$(PLUTO_VINTAGES),../../../data_raw/dcp_mappluto_archive/$(v)/nyc_pluto_$(v).zip) \
	../../../data_raw/dcp_gis_zoning_features/202608/nycgiszoningfeatures_202608shp.zip

../output/mappluto_files.csv: fetch_mappluto_archive.R ../../_lib/source_pipeline_utils.R ../input/source_catalog.csv | ../output
	Rscript --vanilla $<

# Archived tabular PLUTO releases (DCP BYTES); the file name carries the release tag.
../../../data_raw/dcp_mappluto_archive/%.zip:
	mkdir -p $(dir $@)
	wget -q -O $@.part https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pluto/$(notdir $@)
	unzip -tq $@.part
	$(SHA_CHECK) $@.part $(notdir $@)
	mv $@.part $@

../../../data_raw/dcp_gis_zoning_features/202608/nycgiszoningfeatures_202608shp.zip:
	mkdir -p $(dir $@)
	wget -q -O $@.part https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/gis-zoning-features/nycgiszoningfeatures_202608shp.zip
	unzip -tq $@.part
	$(SHA_CHECK) $@.part $(notdir $@)
	mv $@.part $@

../input/source_catalog.csv: ../../source_registry/output/source_catalog.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
