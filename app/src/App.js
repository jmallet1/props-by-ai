import './App.css';
import NBAPlayerPage from './pages/NBAPlayerPage/NBAPlayerPage';
import NotFound from './pages/NotFound/NotFound';
import Home from './pages/Home/Home';
import NavBar from './components/NavBar/NavBar';
import React, { useEffect, useState } from "react";
import { Routes, Route, useLocation } from 'react-router-dom';

function App() {

  const location = useLocation();

  const [playerList, setPlayerList] = useState([]);
  // Fetch data from the API
  const fetchData = async () => {
        
    try {
      // Call API to get player ids and names
      const response = await fetch(`${process.env.AWS_API_CALL_SEARCH_LIST}`);
      const data = await response.json();
        
      // Set the fetched data in the state
      setPlayerList(data);
    } catch (error) {
        console.log('Error fetching list of player IDs');
    }
  };

  useEffect(() => {
      fetchData(); // Call fetchData when the component mounts
  }, []);

  useEffect(() => {
    // Change background color based on the current route
    switch (location.pathname) {
      case "/":
        document.body.style.backgroundColor = "#ffffff";
        break;
      default:
        document.body.style.backgroundColor = "#121212";
    }
  }, [location]);

  return (
    <div className='root'>
      <NavBar playerList={playerList} />
      <div className="ContentWrapper">
        <Routes>
          <Route path="/" element={<Home playerList={playerList} />} caseSensitive={false} />
          <Route path="/player/:playerId" element={<NBAPlayerPage />} caseSensitive={false} />
          <Route path="*" element={<NotFound />} caseSensitive={false} />
        </Routes>
      </div>
    </div>
  );
}

export default App;
