def estimate_join_card(card1, card2, null1, null2, distinct1, distinct2):
    """
    selectivity = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    (selectivity for each possible value of the inner (big) table)
    card = card1*card2*selectivity
    @param card1:
    @param card2:
    @param null2:
    @param distinct1:
    @param distinct2:
    @return:
    """
    rho = (1-null1)*(1-null2)*min(1/distinct1, 1/distinct2)
    card = card1*card2*rho
    return card
