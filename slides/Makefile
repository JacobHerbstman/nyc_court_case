# --- DEFINITIONS ---

SLIDES_SECTIONS := $(shell grep '\\input{' slides.tex | grep -v '%.*\\input' | sed 's/^.*\\input{\(.*\.tex\).*/\1/')
TEX_INPUTS := $(shell grep '\\input{.*\.tex}' $(SLIDES_SECTIONS) | grep -v '%.*\\input' | sed 's/^.*\\input{\(.*\.tex\).*/\1/')
IMG_INPUTS_RAW := $(shell grep '\\includegraphics' $(SLIDES_SECTIONS) | grep -v '%.*\\includegraphics' | sed 's/^.*\\includegraphics\[.*\]{\([^}]*\)}.*/\1/')
IMG_INPUTS := $(foreach f,$(IMG_INPUTS_RAW),$(if $(filter ../../%,$(f)),$(patsubst ../../%,../%,$(f)),$(f)))

all: slides.pdf

clean:
	rm -f *.log *.aux *.out *.toc *.snm *.nav *.bbl *.blg *.fls *.fdb_latexmk

# --- Build slides ---

slides.pdf: slides.tex Makefile $(SLIDES_SECTIONS) $(TEX_INPUTS) $(IMG_INPUTS) \
	figures/institutional_timeline_units_total_ma3.csv \
	../paper/sections/nyc_court_case.bib
	rm -f slides.log slides.aux slides.out slides.toc slides.snm *.nav slides.bbl slides.blg
	pdflatex -interaction=nonstopmode -draftmode $<
	if grep -q '^\\bibdata' slides.aux; then bibtex slides; fi
	pdflatex -interaction=nonstopmode -draftmode $< || true
	pdflatex -interaction=nonstopmode $<
	rm -f slides.log slides.aux slides.out slides.toc slides.snm *.nav slides.bbl slides.blg

figures/institutional_timeline_units_total_ma3.csv: ../tasks/summarize_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_tercile_year_ma3.csv | figures
	awk -F, 'BEGIN { OFS = ","; print "year,low,middle,high" } $$1 == "units_built_total" { year = $$3; values[year "," $$4] = $$16; years[year] = 1 } END { for (year = 1980; year <= 2025; year++) if (year in years) print year, values[year ",1"], values[year ",2"], values[year ",3"] }' $< > $@

figures:
	mkdir -p figures

../tasks/%:
	$(MAKE) -C $(subst output/,code/,$(dir $@)) ../output/$(notdir $@)
