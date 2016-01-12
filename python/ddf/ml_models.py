from __future__ import unicode_literals

import pandas as pd

import util


class Model(object):

    def __init__(self, jml_model, gateway_client):
        self._jml_model = jml_model
        self._gateway_client = gateway_client

    def predict(self, data):
        """
        Predict the result of a sample using this ML model

        :param data:  the candidate sample data to be predicted, vector is expected
        :return: predict result, class tag for classification,
        """
        return self._jml_model.predict(util.to_java_array(data, self._gateway_client.jvm.double, self._gateway_client))


class KMeansModel(Model):

    def __init__(self, jml_model, gateway_client, col_names):
        super(KMeansModel, self).__init__(jml_model, gateway_client)
        self.centers = pd.DataFrame(data=[list(x.toArray()) for x in self._jml_model.getRawModel().clusterCenters()],
                                    columns=col_names)

    def summary(self, print_out=True):
        s = 'KMeans model with {} centers:\n\n{}'.format(len(self.centers), self.centers)
        if print_out:
            print s
        else:
            return s

    def __repr__(self):
        return 'KMeansModel({} clusters)'.format(len(self.centers))

    def __str__(self):
        return 'KMeansModel({} clusters)'.format(len(self.centers))


class GeneralizedLinearModel(Model):

    def __init__(self, jml_model, gateway_client, weights):
        super(GeneralizedLinearModel, self).__init__(jml_model, gateway_client)
        self.weights = weights


class LinearRegressionModel(GeneralizedLinearModel):

    def __init__(self, jml_model, gateway_client, weights):
        super(LinearRegressionModel, self).__init__(jml_model, gateway_client, weights)

    def summary(self, print_out=True):
        s = 'Linear regression model:\n\n{}'.format(self.weights)
        if print_out:
            print s
        else:
            return s

    def __repr__(self):
        return 'LinearRegressionModel({} features)'.format(len(self.weights.columns))

    def __str__(self):
        return 'LinearRegressionModel({} features)'.format(len(self.weights.columns))


class LogisticRegressionModel(GeneralizedLinearModel):

    def __init__(self, jml_model, gateway_client, weights):
        super(LogisticRegressionModel, self).__init__(jml_model, gateway_client, weights)

    def summary(self, print_out=True):
        s = 'Logistic regression model:\n\n{}'.format(self.weights)
        if print_out:
            print s
        else:
            return s

    def __repr__(self):
        return 'LogisticRegressionModel({} features)'.format(len(self.weights.columns))

    def __str__(self):
        return 'LogisticRegressionModel({} features)'.format(len(self.weights.columns))