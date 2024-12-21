import React from 'react';
import { Doughnut } from 'react-chartjs-2';

//register the elements for the Doughnut Chart. More info here: https://www.chartjs.org/docs/latest/getting-started/integration.html
import { Chart as ChartJS, ArcElement, Tooltip } from "chart.js";
ChartJS.register(ArcElement, Tooltip);

const DonutChart = ({overData, underData}) => {

  const data = {
    labels: ['', ''],
    datasets: [
      {
        data: [overData, underData],
        backgroundColor: ['#00E676', '#f44336'],
        hoverBackgroundColor: ['#00E676', '#f44336'],
      },
    ],
  };

  const options = {
    maintainAspectRatio: true,
    cutout: '70%',
    plugins: {
      legend: {
          display: false
      }
    },
  };

  return <Doughnut data={data} options={options} />;
};

export default DonutChart;