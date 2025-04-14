import logging
from abc import abstractmethod
import itertools

import numpy
import numpy as np

import constants


class C3UCBBaseBandit:
    def __init__(self, context_size, hyper_alpha, hyper_lambda, oracle):
        self.arms = []
        self.alpha_original = hyper_alpha
        self.hyper_alpha = hyper_alpha
        self.hyper_lambda = hyper_lambda        # lambda in C2CUB
        self.v = hyper_lambda * numpy.identity(context_size)    # identity matrix of n*n
        self.b = numpy.zeros((context_size, 1))  # [0, 0, ..., 0]T (column matrix) size = number of arms
        self.oracle = oracle
        self.context_vectors = []
        self.upper_bounds = []
        self.context_size = context_size

    @abstractmethod
    def select_arm(self, context_vectors, current_round):
        pass

    @abstractmethod
    def update(self, played_arms, reward, index_use):
        pass


class C3UCB(C3UCBBaseBandit):
    model_count = itertools.count()
    def __init__(self, context_size, hyper_alpha, hyper_lambda, oracle, t = 0,
             delta2=-1, tau=3):
        super().__init__(context_size, hyper_alpha, hyper_lambda, oracle)
        self.model_id = next(C3UCB.model_count)
        self.round_created = t
        self.UCB = 0
        self.LCB = 0
        if delta2 == 0.002:
            self.delta_2 = 0.5
            self.delta_2 = 0.02
            # self.delta_2 = 0.002
            # self.delta_2 = 0.001
            # self.delta_2 = 0.04
            # self.delta_2 = 0.001
            # self.delta_2 = 0.0001
            # self.delta_2 = 0.00001
            # self.delta_2 = 0.000001
            # self.delta_2 = 0.0000001
            # self.delta_2 = 0.00000001
        else:
            self.delta_2 = delta2
        self.d = context_size  # the dim of context features
        self.m = len(self.context_vectors)  # the number of candidate arms
        self.S = 1  # the magnitude of weight vector
        self.V_t = self.hyper_lambda * np.eye(self.d)
        self.reject_accu = 0
        # self.tau = 5
        # self.tau = 10
        self.tau = tau  # the recent history length

    def get_tau_prime(self, t):
        tau_prime = min(t + 1 - self.round_created, self.tau)
        return tau_prime

    def compute_alpah_t(self, t):
        alpha_t = np.sqrt(self.d * np.log((1 + t * self.m/self.hyper_lambda) / self.delta_2)) + np.sqrt(self.hyper_lambda)*self.S
        return alpha_t

    def update_V(self, x_t):
        self.V += np.outer(x_t, x_t)

    def get_d_t(self, t) -> float:
        tau_prime = self.get_tau_prime(t+1)
        model_d_t = np.sqrt(0.5 / tau_prime * np.log(1.0 / self.delta_2))
        return model_d_t

    def get_error_LCB(self, t):
        model_d_t = self.get_d_t(t)
        lcb_t = self.reject_accu - np.sqrt(np.log(self.tau)) * model_d_t
        return lcb_t

    def get_error_UCB(self, id, t):
        x_t_i = self.context_vectors[id]
        mahalanobis_distance = np.dot(np.dot(x_t_i.transpose(), np.linalg.inv(self.V_t)), x_t_i)
        alpha_t = self.compute_alpah_t(t)
        arm_err_ucb = 2 * alpha_t * mahalanobis_distance
        return arm_err_ucb

    def get_model_e_hat_t_m(self, t):
        tau_prime = self.get_tau_prime(t+1)
        model_badness = 1.0 * self.reject_accu / tau_prime
        return model_badness

    def model_reject(self, est_rs, true_rs, t) -> int:
        for i in est_rs:
            est_r = est_rs[i]
            true_r = true_rs[i]
            arm_ucb = self.get_error_UCB(i, t)
            if (est_r - true_r) > arm_ucb:
                return 1.0
        return 0.0

    def select_arm(self, context_vectors, current_round):
        pass

    def select_arm_v2(self, context_vectors):
        """
        This method is responsible for returning the super arm

        :param context_vectors: context vector for this round
        :param current_round: current round number
        :return: selected set of arms
        """
        v_inverse = numpy.linalg.inv(self.v)
        weight_vector = v_inverse @ self.b
        logging.info(f"================================\n{weight_vector.transpose().tolist()[0]}")
        self.context_vectors = context_vectors

        # find the upper bound for every arm
        for i in range(len(self.arms)):
            creation_cost = weight_vector[1] * self.context_vectors[i][1]
            average_reward = (weight_vector.transpose() @ self.context_vectors[i]).item() - creation_cost
            temp_upper_bound = average_reward + self.hyper_alpha * numpy.sqrt(
                (self.context_vectors[i].transpose() @ v_inverse @ self.context_vectors[i]).item())
            temp_upper_bound = temp_upper_bound + (creation_cost / constants.CREATION_COST_REDUCTION_FACTOR)
            self.upper_bounds.append(temp_upper_bound)

        logging.debug(self.upper_bounds)
        self.hyper_alpha = self.hyper_alpha / constants.ALPHA_REDUCTION_RATE
        chosen_ids = self.oracle.get_super_arm(self.upper_bounds, self.arms)
        chosen_arm_est_rewards = {id: self.upper_bounds[id] for id in chosen_ids}
        return chosen_ids, chosen_arm_est_rewards

    def update_v4(self, played_arms, arm_rewards):
        """
        This method can be used to update the reward after each play (improvements required)

        :param played_arms: list of played arms (super arm)
        :param arm_rewards: tuple (gains, creation cost) reward got form playing each arm
        """
        for i in played_arms:
            if self.arms[i].index_name in arm_rewards:
                arm_reward = arm_rewards[self.arms[i].index_name]
            else:
                arm_reward = (0, 0)
            logging.info(f"reward for {self.arms[i].index_name}, {self.arms[i].query_ids_backup} is {arm_reward}")
            self.arms[i].index_usage_last_batch = (self.arms[i].index_usage_last_batch + arm_reward[0]) / 2

            temp_context = numpy.zeros(self.context_vectors[i].shape)
            temp_context[1] = self.context_vectors[i][1]
            self.context_vectors[i][1] = 0

            self.v = self.v + (self.context_vectors[i] @ self.context_vectors[i].transpose())
            self.b = self.b + self.context_vectors[i] * arm_reward[0]

            self.v = self.v + (temp_context @ temp_context.transpose())
            self.b = self.b + temp_context * arm_reward[1]

        self.context_vectors = []
        self.upper_bounds = []

    def set_arms(self, bandit_arms):
        """
        This can be used to initially set the bandit arms in the algorithm

        :param bandit_arms: initial set of bandit arms
        :return:
        """
        self.arms = bandit_arms

    def hard_reset(self):
        """
        Resets the bandit
        """
        self.hyper_alpha = self.alpha_original
        self.v = self.hyper_lambda * numpy.identity(self.context_size)  # identity matrix of n*n
        self.b = numpy.zeros((self.context_size, 1))  # [0, 0, ..., 0]T (column matrix) size = number of arms

    def workload_change_trigger(self, workload_change):
        """
        This forgets history based on the workload change
        if the workload_change > 0.5, hard reset the bandit,
            i.e., reset the value of v and b to the initial ones
        else modify the v and b according to the value of workload_change
        :param workload_change: Percentage of new query templates added (0-1) 0: no workload change, 1: 100% shift
        """
        logging.info("Workload change identified " + str(workload_change))
        if workload_change > 0.5:
            self.hard_reset()
        else:
            forget_factor = 1 - workload_change * 2
            if workload_change > 0.1:
                self.hyper_alpha = self.alpha_original
            self.v = self.hyper_lambda * numpy.identity(self.context_size) + forget_factor * self.v
            self.b = forget_factor * self.b
