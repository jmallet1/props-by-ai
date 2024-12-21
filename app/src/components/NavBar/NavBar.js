import React from 'react';
import SearchBar from '../SearchBar/SearchBar';
import { useLocation } from 'react-router-dom';
import { Link } from 'react-router-dom';
import NavLogo from '../../assets/pictures/white-logo.svg';
import './NavBar.css';

const playerNames = [
  { id: 2544, name: 'LeBron James'}
]; // List of player names

export const NavBar = ({ playerList }) => {

  const location = useLocation();
  const dontShowSearchOnPaths = ['/'];


  return (
    <nav className="navbar">
      <Link to="/" className="brand-logo"><img src={NavLogo} alt='Wager Wiser'/></Link>
      <div className="navbar-center">
        {!dontShowSearchOnPaths.includes(location.pathname) && <SearchBar playerList={playerList} />}
      </div>
    </nav>
  );
};

export default NavBar;