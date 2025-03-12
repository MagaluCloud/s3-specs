import pandas as pd
import plotly.express as px

import tempfile


class DataPlotter:
    def __init__(self, test_data_base: type):
        self.execution_entity = test_data_base.execution_entity
        #self.artifact_info = test_data_base.artifact_info
        self.tests = test_data_base.tests
        self.execution_time = test_data_base.execution_time
        self.failures = test_data_base.failures

        # Ensure types are correct
        assert isinstance(self.execution_entity, pd.DataFrame), "Execution Entity must be a df"
        #assert isinstance(self.artifact_info, pd.DataFrame), "Artifact Info must be a df"
        assert isinstance(self.tests, pd.DataFrame), "Tests must be a df"
        assert isinstance(self.execution_time, pd.DataFrame), "Execution Time must be a df"
        assert isinstance(self.failures, pd.DataFrame), "Failures must be a df"

    def test_name_error_distribution_pie_chart(self):
        failures_df = self.failures

        # Check if failures_df is empty
        if failures_df.empty:
            # Create a placeholder plot with a message
            fig = px.pie(
                names=["No errors or failures"], 
                values=[1], 
                title="Distribuição de falhas por teste",
                color_discrete_sequence=['lightgray']  # Use a neutral color
            )
            fig.update_traces(
                textinfo='none',  # Hide text inside the pie
                hoverinfo='none'  # Disable hover info
            )
            fig.update_layout(
                annotations=[dict(
                    text="No errors or failures", 
                    x=0.5, 
                    y=0.5, 
                    font_size=20, 
                    showarrow=False
                )],
                margin=dict(l=20, r=20, t=40, b=20)  # Adjust margins
            )
        else:
            # Chart needs only Test_Names and their frequency
            test_num_failures = failures_df.Test_Name.value_counts().to_dict()
            
            # Get names and values
            names = list(test_num_failures.keys())
            values = list(test_num_failures.values())

            # Create the pie chart
            fig = px.pie(
                names=names, 
                values=values, 
                title="Distribuição de falhas por teste",
                color_discrete_sequence=px.colors.sequential.RdBu,
            )

            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20)  # Adjust margins if needed
            )

            # Make the pie chart circle bigger by adjusting the marker size
            fig.update_traces(
                marker=dict(line=dict(color='white', width=2)),  # Optional: Add a white border
                textposition='inside',  # Display text inside the slices
                textinfo='percent+label'  # Show percentage and label
            )
        
        # Save the figure to a temporary file
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            fig.write_image(tmpfile.name, format="png", width=800, height=400)
            return tmpfile.name
        
    def plot_category_errors_bar(self):
        failures_df = self.failures.copy()

        # Check if failures_df is empty
        if failures_df.empty:
            # Create a placeholder plot with a message
            fig = px.bar(title="Distribuição de erros por categoria")
            fig.update_layout(
                annotations=[dict(
                    text="No errors or failures", 
                    x=0.5, 
                    y=0.5, 
                    font_size=20, 
                    showarrow=False
                )],
                xaxis_title="Categoria",
                yaxis_title="Contagem Total",
                margin=dict(l=20, r=20, t=40, b=20)  # Adjust margins
            )
        else:
            # Merge failures_df with self.tests
            categories_df = failures_df.merge(
                self.tests,
                how='inner',
                right_on=['Name', 'Execution_Datetime'],
                left_on=['Test_Name', 'Execution_Datetime']
            ).drop_duplicates()

            # Group by Category and Error, then unstack
            categories_df = (
                categories_df.groupby(by=['Category', 'Error'])
                .size()
                .unstack('Error')
                .fillna(0)
                .astype(int)
            )

            # Plot the data
            fig = px.bar(
                categories_df, 
                x=categories_df.index, 
                y=categories_df.columns, 
                barmode='stack', 
                color_discrete_sequence=px.colors.sequential.RdBu
            )

            # Customize the plot
            fig.update_layout(
                title="Distribuição de erros por categoria",
                xaxis_title="Categoria",
                yaxis_title="Contagem Total"
            )

        # Save the plot to a temporary file
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            fig.write_image(tmpfile.name, format="png", width=800, height=400)
            return tmpfile.name
        
    
    def categories_failures_passed_rate(self):
        # Group by status and category, then calculate value counts
        total_category = self.tests.groupby(['Category', 'Status']).size().unstack(fill_value=0).astype(int)

        # Calculate total tests per category
        total_category['TOTAL'] = total_category.sum(axis=1)


        # Calculate percentage for each status
        for status in total_category.columns:
            if status != 'TOTAL':
                total_category[f'{status}_PCT'] = (total_category[status] / total_category['TOTAL']) * 100

        # Prepare data for plotting
        plot_data = []
        for category in total_category.index:
            for status in total_category.columns:
                if status != 'TOTAL' and not status.endswith('_PCT'):
                    plot_data.append({
                        'Category': category,
                        'Status': status,
                        'Percentage': total_category.loc[category, f'{status}_PCT'] if f'{status}_PCT' in total_category.columns else 0,
                        'Real Value': total_category.loc[category, status]
                    })

        plot_df = pd.DataFrame(plot_data)

        # Create a stacked bar plot
        fig = px.bar(
            plot_df, 
            x="Category", 
            y="Percentage", 
            color="Status", 
            barmode='stack', 
            title="Proporção de testes por status",
            labels={'Percentage': 'Porcentagem'},
            text=plot_df["Real Value"].round(0).astype(int),   # Display real values on bars
            color_discrete_sequence=px.colors.qualitative.G10,
        )

        # Adjust layout to display values inside bars
        fig.update_traces(texttemplate='%{text}', textposition='inside')
        fig.update_yaxes(title='Porcentagem (%)')
        fig.update_xaxes(title='Categoria')

        # Display the plot
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
            fig.write_image(tmpfile.name, format="png", width=800, height=400)
            return tmpfile.name