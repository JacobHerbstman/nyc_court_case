all: paper.pdf

paper.pdf: paper.tex sections/*.tex sections/nyc_court_case.bib aea.bst \
	../tasks/build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_map.pdf \
	../tasks/summarize_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_plots.pdf \
	../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf \
	../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_long_difference.tex
	latexmk -pdf -pdflatex="pdflatex -interaction=nonstopmode" -use-make paper.tex

../tasks/build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_map.pdf: FORCE
	$(MAKE) -C ../tasks/build_ccd2010_homeownership_1990_measure/code ../output/ccdist2010_homeownership_1990_map.pdf

../tasks/summarize_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_plots.pdf: FORCE
	$(MAKE) -C ../tasks/summarize_ccd2010_homeownership_long_units_series/code ../output/ccdist2010_homeownership_long_units_plots.pdf

../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf: FORCE
	$(MAKE) -C ../tasks/estimate_ccd2010_homeownership_long_units_event_study/code ../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf

../tasks/estimate_ccd2010_homeownership_long_units_event_study/output/ccdist2010_homeownership_long_units_long_difference.tex: FORCE
	$(MAKE) -C ../tasks/estimate_ccd2010_homeownership_long_units_event_study/code ../output/ccdist2010_homeownership_long_units_long_difference.tex

.PHONY: FORCE clean
FORCE:

clean:
	latexmk -CA
