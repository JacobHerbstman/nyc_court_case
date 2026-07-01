all: paper.pdf

paper.pdf: paper.tex sections/*.tex sections/nyc_court_case.bib aea.bst \
	input/ccdist2010_homeownership_1990_map.pdf \
	input/ccdist2010_homeownership_long_units_raw_units_plots.pdf \
	input/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf \
	input/ccdist2010_homeownership_long_units_long_difference_raw_units.tex
	pdflatex -interaction=nonstopmode paper.tex
	bibtex paper
	pdflatex -interaction=nonstopmode paper.tex
	pdflatex -interaction=nonstopmode paper.tex

input/ccdist2010_homeownership_1990_map.pdf: ../tasks/build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_map.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/ccdist2010_homeownership_long_units_raw_units_plots.pdf: ../tasks/summarize_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_raw_units_plots.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf: ../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/ccdist2010_homeownership_long_units_long_difference_raw_units.tex: ../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_long_difference_raw_units.tex | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

../tasks/build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_map.pdf:
	$(MAKE) -C ../tasks/build_ccd2010_homeownership_1990_measure/code ../output/ccdist2010_homeownership_1990_map.pdf

../tasks/summarize_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_raw_units_plots.pdf:
	$(MAKE) -C ../tasks/summarize_ccd2010_homeownership_long_units_series/code ../output/ccdist2010_homeownership_long_units_raw_units_plots.pdf

../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf:
	$(MAKE) -C ../tasks/estimate_ccd2010_homeownership_long_units_event_study/code ../output/ccdist2010_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf

../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_long_difference_raw_units.tex:
	$(MAKE) -C ../tasks/estimate_ccd2010_homeownership_long_units_event_study/code ../output/ccdist2010_homeownership_long_units_long_difference_raw_units.tex

input:
	mkdir -p "$@"

.PHONY: clean

clean:
	rm -f paper.aux paper.bbl paper.blg paper.log paper.out paper.pdf
