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
                    },
                    ticks: {
                        source: 'auto',
                        autoSkip: false,
                        maxRotation: 0,
                        major: {
                            enabled: true
                        },
                        font: function(context) {
                            if (context.tick && context.tick.major) {
                                return {
                                    weight: 'boldl'
                                };
                            }
                        }
                    }
                },
                y: {
                    beginAtZero: false
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        title: function(context) {
                            return new Date(context[0].parsed.x).toLocaleDateString('en-US', { 
                                year: 'numeric', 
                                month: 'long', 
                                day: 'numeric' 
                            });
                        }
                    }
                }
            }
        }
    });
}

function updateChart(data) {
    stockChart.data.datasets[0].data = data.map(d => ({
        x: new Date(d.Date),
        y: d.Close
    }));
    stockChart.data.datasets[0].label = document.getElementById('symbol-input').value + ' Stock Price';

    const closes = data.map(d => d.Close).filter(c => c !== null);
    const minClose = Math.min(...closes);
    const maxClose = Math.max(...closes);
    const padding = (maxClose - minClose) * 0.1;

    stockChart.options.scales.y.min = minClose - padding;
    stockChart.options.scales.y.max = maxClose + padding;

    // Determine the time span
    const dates = data.map(d => new Date(d.Date));
    const minDate = new Date(Math.min(...dates));
    const maxDate = new Date(Math.max(...dates));
    const timeSpan = (maxDate - minDate) / (1000 * 60 * 60 * 24 * 365); // in years

    if (timeSpan > 2) {
        // If time span is greater than 2 years, label by years
        stockChart.options.scales.x.ticks.callback = function(value, index, values) {
            const date = new Date(value);
            // Check if this tick is the first day of a year
            if (date.getMonth() === 0 && date.getDate() === 1) {
                return date.getFullYear();
            }
            return '';
        };

        // Generate an array of year start dates within the data range
        const yearStarts = [];
        for (let year = minDate.getFullYear(); year <= maxDate.getFullYear(); year++) {
            const yearStart = new Date(year, 0, 1);
            if (yearStart >= minDate && yearStart <= maxDate) {
                yearStarts.push(yearStart);
            }
        }

        // Set these year starts as major ticks
        stockChart.options.scales.x.ticks.major = {
            enabled: true
        };
        stockChart.options.scales.x.ticks.source = 'data';

        // Ensure these year starts are included in the ticks
        stockChart.options.scales.x.min = minDate;
        stockChart.options.scales.x.max = maxDate;
        stockChart.options.scales.x.ticks.includeBounds = true;

    } else {
        // For shorter time spans, use a more detailed labeling
        stockChart.options.scales.x.ticks.callback = function(value, index, values) {
            const date = new Date(value);
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        };
        // Reset to default ticks for shorter time spans
        stockChart.options.scales.x.ticks.source = 'auto';
        stockChart.options.scales.x.min = undefined;
        stockChart.options.scales.x.max = undefined;
    }

    stockChart.update();
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
            return response.text().then(text => {
                console.log('Raw response:', text);  // Log the raw response
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return text;
            });
        })
        .then(text => {
            const data = JSON.parse(text);
            console.log("Parsed data:", data.slice(0, 5));  // Log first 5 entries of parsed data
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

