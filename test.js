const axios = require('axios');

async function sendRequests(userId, count) {
    const promises = [];
    for (let i = 0; i < count; i++) {
        promises.push(
            axios.post('http://localhost:3000/api/v1/task', { user_id: userId })
                .then(response => console.log(`Response: ${response.data.message}`))
                .catch(error => console.log(`Error: ${error.response.data.error}`))
        );
    }
    await Promise.all(promises);
}

async function main() {
    console.log('Sending requests...');
    await sendRequests('123', 1);
}

main();
