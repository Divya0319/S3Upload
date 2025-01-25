document.getElementById('upload-form').addEventListener('submit', function (event) {
    event.preventDefault(); // Prevent the form from submitting immediately

    const fileInput = document.getElementById('file');
    const file = fileInput.files[0];

    if (!file) {
        alert('Please select a file to upload!');
        return;
    }

    if(file.size > 5 * 1024 * 1024) {
        registerEventSource(file);
    } else {
        this.submit();
    }

});

function registerEventSource(file) {

    const fileName = file.name; // Get the file name
    const progressLog = document.getElementById('progress-log');
    progressLog.innerHTML = ''; // Clear previous logs

    const partSize = 5 * 1024 * 1024;
    const totalParts = Math.ceil(file.size / partSize);

    for (let i = 1; i <= totalParts; i++) {
        const progressEntry = document.createElement('div');
        progressEntry.id = `part-${i}`;
        progressEntry.classList.add("progress-container");

        const progressBar = document.createElement('div');
        progressBar.id = `progress-bar-${i}`
        progressBar.classList.add("progress-bar");
        progressBar.textContent = "0%";

        progressEntry.appendChild(progressBar)
        progressLog.appendChild(progressEntry);
    }


    // Create EventSource for monitoring upload progress
    const eventSource = new EventSource(`/progress/${encodeURIComponent(fileName)}`);

    eventSource.onmessage = function (event) {
        const data = JSON.parse(event.data);
        const { partNumber, percentage } = data;

        // Update or create progress entry for the current part
        let progressBar = document.getElementById(`progress-bar-${partNumber}`);
        if (progressBar) {
            progressBar.style.width = `${percentage}%`;
            progressBar.textContent = `${percentage.toFixed(1)}%`;
        }
    };

    eventSource.onerror = function () {
        const errorMessage = document.createElement('p');
        errorMessage.textContent = 'Error in progress monitoring!';
        errorMessage.className = 'text-danger';
        progressLog.appendChild(errorMessage);
        eventSource.close();
    };

    // Submit the form programmatically after setting up the EventSource
    document.getElementById('upload-form').submit();
}