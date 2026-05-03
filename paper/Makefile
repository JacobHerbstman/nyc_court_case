all: paper.pdf

paper.pdf: paper.tex sections/*.tex sections/nyc_court_case.bib aea.bst \
	../tasks/summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_plots.pdf \
	../tasks/summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_plots.pdf \
	../tasks/redevelopment_potential_first_pass/output/figures/redev_potential_maps.pdf \
	../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf \
	../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex \
	../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex
	latexmk -pdf -pdflatex="pdflatex -interaction=nonstopmode" -use-make paper.tex

../tasks/summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_plots.pdf: FORCE
	$(MAKE) -C ../tasks/summarize_cd_homeownership_long_units_series/code ../output/cd_homeownership_long_units_plots.pdf

../tasks/summarize_brooklyn_homeownership_case_study/output/brooklyn_homeownership_case_study_plots.pdf: FORCE
	$(MAKE) -C ../tasks/summarize_brooklyn_homeownership_case_study/code ../output/brooklyn_homeownership_case_study_plots.pdf

../tasks/redevelopment_potential_first_pass/output/figures/redev_potential_maps.pdf: FORCE
	$(MAKE) -C ../tasks/redevelopment_potential_first_pass/code ../output/figures/redev_potential_maps.pdf

../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf: FORCE
	$(MAKE) -C ../tasks/estimate_cd_homeownership_long_units_event_study/code ../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf

../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex: FORCE
	$(MAKE) -C ../tasks/estimate_cd_homeownership_long_units_event_study/code ../output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex

../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex: FORCE
	$(MAKE) -C ../tasks/estimate_cd_homeownership_long_units_event_study/code ../output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex

.PHONY: FORCE clean
FORCE:

clean:
	latexmk -CA
