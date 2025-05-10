import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import './App.css';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const App = () => {
  const [symbol, setSymbol] = useState('AAPL');
  const [chartData, setChartData] = useState({});

  const fetchData = async () => {
    try {
      const response = await axios.get(`http://localhost:5000/api/stock_data/${symbol}`);
      const data = response.data;

      const labels = data.map(item => new Date(item.date).toLocaleDateString());
      const closePrices = data.map(item => item.close);
      const volumes = data.map(item => item.volume);

      setChartData({
        labels: labels.reverse(),
        datasets: [
          {
            label: `${symbol} Close Price`,
            data: closePrices.reverse(),
            borderColor: 'rgb(75, 192, 192)',
            tension: 0.1,
          },
          {
            label: `${symbol} Volume`,
            data: volumes.reverse(),
            borderColor: 'rgb(255, 99, 132)',
            tension: 0.1,
            yAxisID: 'y1',
          },
        ],
      });
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60000); // Poll every 60 seconds
    return () => clearInterval(interval);
  }, [symbol]);

  const options = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: `${symbol} Stock Data (Real-Time)`,
      },
    },
    scales: {
      y: {
        type: 'linear',
        display: true,
        position: 'left',
        title: {
          display: true,
          text: 'Close Price ($)',
        },
      },
      y1: {
        type: 'linear',
        display: true,
        position: 'right',
        title: {
          display: true,
          text: 'Volume',
        },
        grid: {
          drawOnChartArea: false,
        },
      },
    },
  };

  return (
    <div style={{ padding: '20px' }}>
      <h1>Real-Time Stock Data Visualization</h1>
      <select onChange={(e) => setSymbol(e.target.value)} value={symbol}>
        {['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA'].map(t => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>
      {chartData.labels && (
        <Line options={options} data={chartData} />
      )}
    </div>
  );
};

export default App;