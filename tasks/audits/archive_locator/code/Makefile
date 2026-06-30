all: ../output/archive_locator_queue.csv

../output/archive_locator_queue.csv: archive_locator.R ../input/archive_requests.csv | ../output
	Rscript --vanilla $<

../input/archive_requests.csv: ../../source_registry_context/code/archive_requests.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/archive_requests.csv

include ../../generic.make
