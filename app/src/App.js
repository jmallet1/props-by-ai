import './App.css';
import NBAPlayerPage from './pages/NBAPlayerPage/NBAPlayerPage';
import NotFound from './pages/NotFound/NotFound';
import Home from './pages/Home/Home';
import NavBar from './components/NavBar/NavBar';
import React, { useEffect } from "react";
import { BrowserRouter as Router, Routes, Route, useLocation } from 'react-router-dom';

function App() {

  const location = useLocation();

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
      <NavBar />
      <div className="ContentWrapper">
        <Routes>
          <Route path="/" element={<Home />} caseSensitive={false} />
          <Route path="/player/player-1" element={<NBAPlayerPage />} caseSensitive={false} />
          <Route path="*" element={<NotFound />} caseSensitive={false} />
        </Routes>
      </div>
    </div>
  );
}

export default App;
