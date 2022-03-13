import logging

from airflow.models import BaseOperator


class PandasOperator(BaseOperator):
    template_fields = (
        "_input_callable_kwargs",
        "_transform_callable_kwargs",
        "_output_callable_kwargs",
    )

    def __init__(
        self,
        input_callable,
        output_callable,
        transform_callable=None,
        input_callable_kwargs=None,
        transform_callable_kwargs=None,
        output_callable_kwargs=None,
        **kwargs
    ):
        super().__init__(**kwargs)

        # attributes for reading data
        self._input_callable = input_callable
        self._input_callable_kwargs = input_callable_kwargs or {}

        # attributes for transformations
        self._transform_callable = transform_callable
        self._transform_callable_kwargs = transform_callable_kwargs or {}

        # attributes for writing data
        self._output_callable = output_callable
        self._output_callable_kwargs = output_callable_kwargs or {}

    def execute(self, context):
        df = self._input_callable(**self._input_callable_kwargs)
        logging.info("Read DataFrame with shape %s", df.shape)

        if self._transform_callable:
            df = self._transform_callable(df, **self._transform_callable_kwargs)
            logging.info("DataFrame shape after transformation %s", df.shape)

        self._output_callable(df, **self._output_callable_kwargs)
