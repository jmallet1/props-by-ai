import './App.css';
import NBAPlayerPage from './pages/NBAPlayerPage/NBAPlayerPage';
import NotFound from './pages/NotFound/NotFound';
import Home from './pages/Home/Home';
import NavBar from './components/NavBar/NavBar';
import Footer from './components/Footer/Footer';
import React, { useEffect, useState } from "react";
import { Routes, Route, useLocation } from 'react-router-dom';

function App() {

  const location = useLocation();

  const [playerList, setPlayerList] = useState([]);
  // Fetch data from the API
  const fetchData = async () => {
        
    try {
      // Call API to get player ids and names
      const response = await fetch(process.env.REACT_APP_search_api);
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

  const [windowWidth, setWindowWidth] = useState(window.innerWidth);
  useEffect(() => {
      // Function to update state when the window is resized
      const handleResize = () => {
          setWindowWidth(window.innerWidth);
      };

      // Add event listener to track window resize
      window.addEventListener('resize', handleResize);

      // Clean up the event listener when the component unmounts
      return () => {
          window.removeEventListener('resize', handleResize);
      };
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

    // Scroll to top when the route changes
    window.scrollTo(0, 0);
  }, [location]);

  return (
    <div className='root'>
      {location.pathname !== "/" && <NavBar playerList={playerList} windowWidth={windowWidth}/> }
      <div className="ContentWrapper">
        <Routes>
          <Route path="/" element={<Home playerList={playerList} />} caseSensitive={false} />
          <Route path="/player/:playerId" element={<NBAPlayerPage windowWidth={windowWidth}/>} caseSensitive={false} />
          <Route path="*" element={<NotFound />} caseSensitive={false} />
        </Routes>
      </div>
      <Footer />
    </div>
  );
}

export default App;
