"""Source examples and ambiguity checks for the shared CPC counting rules."""

import sys
import unittest

sys.path.insert(0, "../../../_lib")
from cpc_counts import board_review, hearing_speakers, prose_review_section


class CPCCounts(unittest.TestCase):
    def test_disapproval_motion_c780349tcm(self):
        row = board_review("The Board adopted a resolution by a vote of 21 in favor, "
                           "2 opposed and 3 abstentions disapproving the proposed cafe.")
        self.assertEqual((row["reported_for"], row["reported_against"]), (21, 2))
        self.assertEqual((row["votes_for"], row["votes_against"], row["abstentions"]), (2, 21, 3))

    def test_failed_approval_c160174zsr(self):
        row = board_review("By a vote of 17 in favor, 14 opposed, and with five abstentions, "
                           "denied the recommendation to approve both applications. "
                           "The abstentions are counted as disapproval votes.")
        self.assertEqual((row["votes_for"], row["votes_against"], row["abstentions"]), (17, 14, 5))
        self.assertEqual((row["position"], row["effective_against"]), ("oppose", 19))

    def test_abstentions_do_not_automatically_become_opposition(self):
        row = board_review("The board recommended approval by a vote of 17 in favor, "
                           "14 opposed and five abstentions.")
        self.assertEqual(row["abstentions"], 5)
        self.assertIsNone(row["effective_against"])
        self.assertEqual(row["votes_against"], 14)

    def test_unknown_motion_preserves_raw_tally(self):
        row = board_review("The board voted 17 to 14.")
        self.assertEqual(row["reported_for"], 17)
        self.assertIsNone(row["votes_for"])
        self.assertEqual(row["status"], "unknown_motion")

    def test_multiple_votes_do_not_get_summed(self):
        row = board_review("The board recommended approval by a vote of 19 in favor, 3 opposed. "
                           "The recommendation was approval by a vote of 19 in favor, 3 opposed.")
        self.assertEqual(row["status"], "multiple_tallies")
        self.assertIsNone(row["votes_for"])

    def test_procedural_vote_is_not_proposal_support(self):
        row = board_review("The board made no recommendation, by a vote of 20 to 4.")
        self.assertEqual(row["reported_for"], 20)
        self.assertIsNone(row["votes_for"])

    def test_closed_hearing_does_not_establish_zero(self):
        row = hearing_speakers("Four speakers in favor appeared. The hearing was closed.")
        self.assertEqual(row["votes_for"], 4)
        self.assertIsNone(row["votes_against"])
        self.assertEqual(row["status"], "partial")

    def test_explicit_absence_of_other_speakers(self):
        row = hearing_speakers("Four speakers in favor appeared. There were no other speakers.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (4, 0))

    def test_zero_for_one_side_is_not_zero_for_both(self):
        row = hearing_speakers("Twenty-one speakers in favor and no speakers against the proposal.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (21, 0))

    def test_extended_number_words_and_ocr(self):
        row = hearing_speakers("S e v e n t y speakers in favor and two in opposition appeared.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (70, 2))
        row = board_review("The board recommended approval by a vote of t w e n t y - o n e "
                           "in favor, 2 opposed.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (21, 2))

    def test_conflicting_written_and_numeric_counts(self):
        row = hearing_speakers("Twenty (21) speakers in favor and two in opposition.")
        self.assertIsNone(row["votes_for"])

    def test_one_hearing_has_one_block(self):
        row = hearing_speakers("The Commission scheduled a hearing. The hearing was duly held. "
                               "There were two speakers in favor and one speaker in opposition.")
        self.assertEqual(row["hearing_count"], 1)
        self.assertEqual((row["votes_for"], row["votes_against"]), (2, 1))

    def test_continued_hearings_keep_the_aggregation_visible(self):
        row = hearing_speakers("The hearing was duly held. Two speakers in favor and one in opposition. "
                               "The continued hearing was duly held. Three speakers in favor and two in opposition.")
        self.assertEqual(row["hearing_count"], 2)
        self.assertEqual(row["status"], "multiple_hearings")
        self.assertEqual((row["votes_for"], row["votes_against"]), (5, 3))

    def test_descriptions_between_count_and_stance(self):
        for phrase, count in [
            ("Two speakers representing the applicant spoke in favor", 2),
            ("Three speakers who spoke in favor", 3),
            ("Two speakers, who spoke in support", 2),
            ("Three speakers from the development team in favor", 3),
            ("Three speakers appeared in favor", 3),
        ]:
            row = hearing_speakers(phrase + " and none opposed. There were no other speakers.")
            self.assertEqual((row["votes_for"], row["votes_against"]), (count, 0), phrase)

    def test_description_does_not_cross_the_opposite_stance(self):
        row = hearing_speakers("Three speakers from the applicant team in favor and five opposed.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (3, 5))

    def test_initial_empty_hearing_is_not_zero_for_continued_hearing(self):
        row = hearing_speakers("The hearing was duly held. There were no appearances and the hearing "
                               "was continued to October 23. There were four speakers in favor.")
        self.assertEqual(row["status"], "continued_hearing")
        self.assertIsNone(row["votes_for"])

    def test_zero_opponents_does_not_establish_supporter_count(self):
        row = hearing_speakers("Several speakers favored the application and none opposed. "
                               "There were no other speakers.")
        self.assertIsNone(row["votes_for"])
        self.assertEqual(row["votes_against"], 0)

    def test_multiple_boards_are_not_one_tally(self):
        row = board_review("Community Board 3 recommended approval by a vote of 24 in favor, "
                           "one opposed. Community Board 16 unanimously recommended approval "
                           "by a vote of 34 in favor.")
        self.assertEqual(row["status"], "multiple_boards")

    def test_disapproval_with_opposing_majority_needs_alignment_review(self):
        row = board_review("The board disapproved the application by a vote of 0 in favor, 18 opposed.")
        self.assertEqual((row["reported_for"], row["reported_against"]), (0, 18))
        self.assertEqual(row["status"], "ambiguous_alignment")

    def test_approval_condition_can_reject_the_actual_site(self):
        row = board_review("By a vote of 23 in favor, none opposed, the board recommended approval "
                           "with the condition that the shelter be located at a different site.")
        self.assertEqual(row["status"], "substantive_conditions")

    def test_prose_sections_do_not_mix_board_and_commission(self):
        text = ("Community Board 2 held a public\nhearing and recommended approval by a vote of "
                "22 in favor, none opposed. The Borough President recommended approval. "
                "The Commission scheduled a public\nhearing. Three speakers in favor and two opposed. "
                "CONSIDERATION The Commission approved the project by a vote of 12 to 1.")
        board = board_review(prose_review_section(text, "community_board"))
        hearing = hearing_speakers(prose_review_section(text, "cpc_hearing"))
        self.assertEqual((board["votes_for"], board["votes_against"]), (22, 0))
        self.assertEqual((hearing["votes_for"], hearing["votes_against"]), (3, 2))

    def test_prose_fallback_requires_an_end_boundary(self):
        text = "The Commission held a public hearing. Three speakers in favor and one opposed."
        self.assertEqual(prose_review_section(text, "cpc_hearing"), "")

    def test_explicit_resolution_variants(self):
        for phrase in ["adopted a resolution to approve the application",
                       "adopted a resolution in favor of the application",
                       "supports the disposition", "approves the application"]:
            row = board_review("The board " + phrase + " by a vote of 27 in favor, two opposed.")
            self.assertEqual((row["votes_for"], row["votes_against"]), (27, 2), phrase)

    def test_noncomplying_positive_vote_is_not_formal_approval(self):
        row = board_review("Community Board #4 voted in favor of the application by a vote of 11-0. "
                           "This did not constitute a majority of the board.")
        self.assertEqual(row["position"], "no_recommendation")
        self.assertEqual(row["status"], "procedural_vote")

    def test_hearing_total_is_not_a_side_count(self):
        row = hearing_speakers("There were sixteen speakers, six in favor and ten in opposition.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (6, 10))

    def test_individuals_who_testified_are_speakers(self):
        row = hearing_speakers("Three speakers spoke in favor. Five individuals spoke in opposition. "
                               "There were no other speakers.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (3, 5))

    def test_unquantified_opposition_is_not_zero(self):
        row = hearing_speakers("Three speakers spoke in favor. Several individuals spoke in opposition. "
                               "There were no other speakers.")
        self.assertIsNone(row["votes_against"])

    def test_multiple_support_groups_require_review(self):
        row = hearing_speakers("There were seven speakers in favor, two speakers in favor with "
                               "modifications, and two speakers in opposition.")
        self.assertEqual(row["status"], "conflicting_counts")
        self.assertIsNone(row["votes_for"])

    def test_equal_sized_groups_are_not_one_group(self):
        row = hearing_speakers("Two speakers spoke in favor and two speakers spoke in favor with "
                               "modifications. There were no other speakers.")
        self.assertEqual(row["status"], "multiple_counts")

    def test_applicant_team_is_not_a_singular_actor(self):
        row = hearing_speakers("An applicant team consisting of three members spoke in favor. "
                               "There were no other speakers.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (3, 0))
        row = hearing_speakers("The applicant team spoke in favor. There were no other speakers.")
        self.assertIsNone(row["votes_for"])

    def test_no_speakers_on_one_side_with_a_verb(self):
        row = hearing_speakers("Five speakers spoke in favor. No speakers appeared in opposition.")
        self.assertEqual((row["votes_for"], row["votes_against"]), (5, 0))


unittest.main()
