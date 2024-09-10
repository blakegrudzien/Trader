let stockChart;

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM fully loaded');
    if (document.querySelector('.historical-data-page')) {
        initializeHistoricalDataPage();
    } else {
        console.log('Not on historical data page');
    }
});

function initializeHistoricalDataPage() {
    console.log('Initializing historical data page');
    const updateButton = document.getElementById('update-data');
    if (updateButton) {
        updateButton.addEventListener('click', function(event) {
            event.preventDefault();
            console.log('Update button clicked');
            fetchHistoricalData();
        });
        console.log('Event listener added to update button');
    } else {
        console.error('Update button not found');
    }

    createChart();
}

function createChart() {
    const ctx = document.getElementById('stock-chart').getContext('2d');
    stockChart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Stock Price',
                data: [],
                borderColor: 'rgb(75, 192, 192)',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day'
                    }
                },
                y: {
                    beginAtZero: false
                }
            }
        }
    });
}

function fetchHistoricalData() {
    const symbol = document.getElementById('symbol-input').value.trim();
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value || new Date().toISOString().split('T')[0];

    if (!symbol) {
        alert('Please enter a stock symbol');
        return;
    }
    if (!startDate) {
        alert('Please enter a start date');
        return;
    }

    const url = `/api/historical-data?symbol=${encodeURIComponent(symbol)}&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`;
    console.log('Fetching data from URL:', url);

    fetch(url)
        .then(response => {
            console.log('Response status:', response.status);
            return response.text();
        })
        .then(text => {
            const data = JSON.parse(text);
            console.log("Received data:", data.slice(0, 5));  // Log first 5 entries
            if (data.error) {
                throw new Error(data.error);
            }
            if (data.length === 0) {
                throw new Error('No data available for the specified date range');
            }
            updateChart(data);
        })
        .catch(error => {
            console.error('Error fetching historical data:', error);
            alert('Failed to fetch data: ' + error.message);
        });
}

function updateChart(data) {
    stockChart.data.datasets[0].data = data.map(d => ({
        x: d.Date,
        y: d.Close
    }));
    stockChart.data.datasets[0].label = document.getElementById('symbol-input').value + ' Stock Price';

    const closes = data.map(d => d.Close).filter(c => c !== null);
    const minClose = Math.min(...closes);
    const maxClose = Math.max(...closes);
    const padding = (maxClose - minClose) * 0.1;

    stockChart.options.scales.y.min = minClose - padding;
    stockChart.options.scales.y.max = maxClose + padding;

    stockChart.update();
}