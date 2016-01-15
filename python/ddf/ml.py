from __future__ import unicode_literals

from py4j import java_gateway
import pandas as pd

from ml_models import KMeansModel, LinearRegressionModel, LogisticRegressionModel
import util


def kmeans(data, centers=2, runs=5, max_iters=10):
    """
    Train Kmeans on a given DDF
    :param data: DDF
    :param centers: number of clusters
    :param runs: number of runs
    :param max_iters: number of iterations
    :return: an object of type KMeansModel
    """
    ml_obj = java_gateway.get_field(data._jddf, 'ML')
    return KMeansModel(ml_obj.KMeans(centers, runs, max_iters), data._gateway_client, data.colnames)


def linear_regression_gd(data, step_size=1.0, batch_fraction=1.0, max_iters=10):
    """
    Linear regression with gradient descent
    :param data:
    :param step_size:
    :param batch_fraction:
    :param max_iters:
    :return:
    """
    ml_obj = java_gateway.get_field(data._jddf, 'ML')
    gateway = data._gateway_client
    model = ml_obj.train('linearRegressionWithSGD',
                         util.to_java_array([max_iters, step_size, batch_fraction],
                                            gateway.jvm.Object, gateway))
    weights = [float(model.getRawModel().intercept())] + list(model.getRawModel().weights().toArray())
    weights = pd.DataFrame(data=[weights], columns=['Intercept'] + data.colnames[:-1])
    return LinearRegressionModel(model, gateway, weights)


def logistic_regression_gd(data, step_size=1.0, max_iters=10):
    """

    :param data:
    :param step_size:
    :param max_iters:
    :return:
    """
    ml_obj = java_gateway.get_field(data._jddf, 'ML')
    gateway = data._gateway_client
    model = ml_obj.train('logisticRegressionWithSGD',
                         util.to_java_array([max_iters, step_size],
                                            gateway.jvm.Object, gateway))
    weights = [float(model.getRawModel().intercept())] + list(model.getRawModel().weights().toArray())
    weights = pd.DataFrame(data=[weights], columns=['Intercept'] + data.colnames[:-1])
    return LogisticRegressionModel(model, gateway, weights)
