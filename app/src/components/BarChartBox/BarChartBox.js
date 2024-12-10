import { Bar } from "react-chartjs-2";
import annotationPlugin from 'chartjs-plugin-annotation';

import {
    Chart as ChartJS, 
    CategoryScale, 
    LinearScale, 
    BarElement, 
    Title,
    Tooltip,
    Legend,
    plugins
} from "chart.js";
import './BarChartBox.css'
import { color } from "chart.js/helpers";

ChartJS.register(
    CategoryScale, 
    LinearScale, 
    BarElement,
    Title,
    Tooltip,
    Legend,
    annotationPlugin
);

const BarChartBox = ({ data, max_value, avgLine, x_labels }) => {

    const split_x_labels = x_labels.map(label => label.split(' '));
    
    const BarChartData = 
    [
        {
            labels: split_x_labels,
            datasets: [
                {
                    data: data,
                    backgroundColor: '#651fff',
                    barPercentage: 0.5,
                    categoryPercentage: 1,
                    borderRadius: window.innerWidth < 600 ? 5 : 10
                },
            ],
        }
    ];

    const options = {
        maintainAspectRatio: false,
        responsive: true,
        plugins: {
            legend: {
                display: false
            },
                annotation: {
                  annotations: {
                    line1: {
                      type: 'line',
                      yMin: avgLine,  // Set this to the y-value where the horizontal line should be
                      yMax: avgLine,
                      borderColor: 'red',
                      borderWidth: 2,
                      label: {
                        content: 'Threshold',
                        enabled: true,
                        position: 'end',
                      },
                    },
                },
            },
        },
        scales: {
            x: {
                ticks: {
                    font: {
                        size: window.innerWidth < 414 ? 5 : window.innerWidth < 768 ? 9 : 14 // Adjusts based on screen size
                    },
                    color: '#E0E0E0'                
                },
                grid: {
                    drawOnChartArea: false
                }
            },
            y: {
                beginAtZero: true,
                ticks: {
                    // forces step size 1/3 of the max
                    stepSize: (max_value + (max_value / 10)) / 3,
                    font: {
                        size: window.innerWidth < 414 ? 6 : window.innerWidth < 768 ? 9 : 14 // Adjusts based on screen size
                    },
                    color: '#E0E0E0'
                }
            }
        }
    };

    return (
        <div className="container">
            <Bar options={options} data={BarChartData[0]} />
        </div>
    );
};

export default BarChartBox;