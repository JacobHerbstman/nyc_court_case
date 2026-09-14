all: paper.pdf

paper.pdf: paper.tex sections/*.tex sections/nyc_court_case.bib aea.bst \
	input/cd_homeownership_1990_map.pdf \
	input/cd_homeownership_long_units_raw_units_plots.pdf \
	input/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf \
	input/cd_homeownership_long_units_long_difference_raw_units.tex
	pdflatex -interaction=nonstopmode paper.tex
	bibtex paper
	pdflatex -interaction=nonstopmode paper.tex
	pdflatex -interaction=nonstopmode paper.tex

input/cd_homeownership_1990_map.pdf: ../tasks/build_cd_homeownership_1990_measure/output/cd_homeownership_1990_map.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/cd_homeownership_long_units_raw_units_plots.pdf: ../tasks/summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_raw_units_plots.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf: ../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/cd_homeownership_long_units_long_difference_raw_units.tex: ../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_long_difference_raw_units.tex | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

../tasks/build_cd_homeownership_1990_measure/output/cd_homeownership_1990_map.pdf:
	$(MAKE) -C ../tasks/build_cd_homeownership_1990_measure/code ../output/cd_homeownership_1990_map.pdf

../tasks/summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_raw_units_plots.pdf:
	$(MAKE) -C ../tasks/summarize_cd_homeownership_long_units_series/code ../output/cd_homeownership_long_units_raw_units_plots.pdf

../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf:
	$(MAKE) -C ../tasks/estimate_cd_homeownership_long_units_event_study/code ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf

../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_long_difference_raw_units.tex:
	$(MAKE) -C ../tasks/estimate_cd_homeownership_long_units_event_study/code ../output/cd_homeownership_long_units_long_difference_raw_units.tex

input:
	mkdir -p "$@"

.PHONY: clean

clean:
	rm -f paper.aux paper.bbl paper.blg paper.log paper.out paper.pdf
