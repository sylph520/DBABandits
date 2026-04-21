import operator
from abc import abstractmethod
import random

import constants


class BaseOracle:

    def __init__(self, max_memory):
        self.max_memory = max_memory

    @abstractmethod
    def get_super_arm(self, q_function, context_vectors, bandit_arms, epsilon):
        pass

    @staticmethod
    def removed_covered_v2(arm_ucb_dict, chosen_id, bandit_arms, remaining_memory):
        """
        second version which is based on the remaining memory and already chosen arms

        :param arm_ucb_dict: dictionary of arms and upper confidence bounds
        :param chosen_id: chosen arm in this round
        :param bandit_arms: Bandit arm list
        :param remaining_memory: max_memory - used_memory
        :return: reduced arm list
        """
        reduced_arm_ucb_dict = {}
        for arm_id in arm_ucb_dict:
            if not (bandit_arms[arm_id] <= bandit_arms[chosen_id] or bandit_arms[arm_id].memory > remaining_memory):
                reduced_arm_ucb_dict[arm_id] = arm_ucb_dict[arm_id]
        return reduced_arm_ucb_dict

    @staticmethod
    def removed_covered_tables(arm_ucb_dict, chosen_id, bandit_arms, table_count):
        """

        :param arm_ucb_dict: dictionary of arms and upper confidence bounds
        :param chosen_id: chosen arm in this round
        :param bandit_arms: Bandit arm list
        :param table_count: count of indexes already chosen for each table
        :return: reduced arm list
        """
        reduced_arm_ucb_dict = {}
        for arm_id in arm_ucb_dict:
            if not (bandit_arms[arm_id].table_name == bandit_arms[chosen_id].table_name and table_count[
                    bandit_arms[arm_id].table_name] >= constants.MAX_INDEXES_PER_TABLE):
                reduced_arm_ucb_dict[arm_id] = arm_ucb_dict[arm_id]
        return reduced_arm_ucb_dict

    @staticmethod
    def removed_covered_clusters(arm_ucb_dict, chosen_id, bandit_arms, enable_cluster_filter=True):
        """
        Remove arms that are in the same cluster as the chosen arm.
        When enable_cluster_filter is False, skip this filtering.

        :param arm_ucb_dict: dictionary of arms and upper confidence bounds
        :param chosen_id: chosen arm in this round
        :param bandit_arms: Bandit arm list
        :param enable_cluster_filter: if False, skip cluster-based filtering
        :return: reduced arm list
        """
        # If cluster filtering is disabled, return all arms
        if not enable_cluster_filter:
            return arm_ucb_dict

        reduced_arm_ucb_dict = {}
        for arm_id in arm_ucb_dict:
            if not (bandit_arms[arm_id].table_name == bandit_arms[chosen_id].table_name and bandit_arms[
                chosen_id].cluster is not None and bandit_arms[arm_id].cluster is not None and bandit_arms[
                        arm_id].cluster == bandit_arms[chosen_id].cluster):
                reduced_arm_ucb_dict[arm_id] = arm_ucb_dict[arm_id]
        return reduced_arm_ucb_dict

    @staticmethod
    def removed_covered_queries_v2(arm_ucb_dict, chosen_id, bandit_arms, enable_query_overlap_filter=True):
        """
        When covering index is selected for a query we gonna remove all other arms from that query.
        When enable_query_overlap_filter is False, skip this filtering.
        
        :param arm_ucb_dict: dictionary of arms and upper confidence bounds
        :param chosen_id: chosen arm in this round
        :param bandit_arms: Bandit arm list
        :param enable_query_overlap_filter: if False, skip query overlap filtering
        :return: reduced arm list
        """
        # If query overlap filtering is disabled, return all arms
        if not enable_query_overlap_filter:
            return arm_ucb_dict

        reduced_arm_ucb_dict = {}
        for arm_id in arm_ucb_dict:
            query_ids = bandit_arms[chosen_id].query_ids
            for query_id in query_ids:
                if (bandit_arms[arm_id].table_name == bandit_arms[chosen_id].table_name and bandit_arms[
                        chosen_id].is_include == 1 and query_id in bandit_arms[arm_id].query_ids):
                    bandit_arms[arm_id].query_ids.remove(query_id)
            if bandit_arms[arm_id].query_ids != set():
                reduced_arm_ucb_dict[arm_id] = arm_ucb_dict[arm_id]
        return reduced_arm_ucb_dict


class OracleDDQN(BaseOracle):

    def get_super_arm(self, q_function, context_vectors, bandit_arms, epsilon, enable_cluster_filter=True, enable_query_overlap_filter=True):
        used_memory = 0
        chosen_arms = []
        arm_ucb_dict = {}
        table_count = {}

        for i in range(len(bandit_arms)):
            arm_ucb_dict[i] = q_function[i]

        while len(arm_ucb_dict) > 0:
            if random.random() < epsilon:
                max_ucb_arm_id = random.sample(list(arm_ucb_dict), 1)[0]
            else:
                max_ucb_arm_id = max(arm_ucb_dict.items(), key=operator.itemgetter(1))[0]

            if bandit_arms[max_ucb_arm_id].memory < self.max_memory - used_memory:
                chosen_arms.append(max_ucb_arm_id)
                used_memory += bandit_arms[max_ucb_arm_id].memory
                if bandit_arms[max_ucb_arm_id].table_name in table_count:
                    table_count[bandit_arms[max_ucb_arm_id].table_name] += 1
                else:
                    table_count[bandit_arms[max_ucb_arm_id].table_name] = 1
                arm_ucb_dict = self.removed_covered_tables(arm_ucb_dict, max_ucb_arm_id, bandit_arms, table_count)
                arm_ucb_dict = self.removed_covered_clusters(arm_ucb_dict, max_ucb_arm_id, bandit_arms, enable_cluster_filter)
                arm_ucb_dict = self.removed_covered_queries_v2(arm_ucb_dict, max_ucb_arm_id, bandit_arms, enable_query_overlap_filter)
                arm_ucb_dict = self.removed_covered_v2(arm_ucb_dict, max_ucb_arm_id, bandit_arms,
                                                       self.max_memory - used_memory)
            else:
                arm_ucb_dict.pop(max_ucb_arm_id)

        return chosen_arms

