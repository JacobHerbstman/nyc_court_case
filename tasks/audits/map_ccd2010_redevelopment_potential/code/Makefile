all: ../output/ccdist2010_redev_2002_map_qc.csv \
	../output/ccdist2010_redev_2002_hex_logsum_map.png \
	../output/ccdist2010_redev_2002_hex_density_map.png \
	../output/ccdist2010_redev_2002_council_map.png \
	../output/ccdist2010_redev_2002_hex_grid.gpkg

../output/ccdist2010_redev_2002_map_qc.csv: map_ccd2010_redevelopment_potential.R ../../../_lib/source_pipeline_utils.R ../input/ccdist2010_homeownership_1990_measure.csv ../input/ccdist2010_redevelopment_potential.csv ../input/mappluto_02b.zip | ../output
	Rscript --vanilla $<

../output/ccdist2010_redev_2002_hex_logsum_map.png: ../output/ccdist2010_redev_2002_map_qc.csv
	@test -s "$@"

../output/ccdist2010_redev_2002_hex_density_map.png: ../output/ccdist2010_redev_2002_map_qc.csv
	@test -s "$@"

../output/ccdist2010_redev_2002_council_map.png: ../output/ccdist2010_redev_2002_map_qc.csv
	@test -s "$@"

../output/ccdist2010_redev_2002_hex_grid.gpkg: ../output/ccdist2010_redev_2002_map_qc.csv
	@test -s "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_redevelopment_potential.csv: ../../../build_ccd2010_redevelopment_potential/output/ccdist2010_redevelopment_potential.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_02b.zip: ../../../../data_raw/dcp_mappluto_archive/02b/mappluto_02b.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_1990_measure.csv ../input/ccdist2010_redevelopment_potential.csv ../input/mappluto_02b.zip

include ../../generic.make
