import React from 'react';
import SearchBar from '../SearchBar/SearchBar';
import { useLocation } from 'react-router-dom';
import { Link } from 'react-router-dom';
import NavLogo from '../../assets/pictures/white-logo.svg';
import LoginButtons from '../LoginButtons/LoginButtons';
import './NavBar.css';

export const NavBar = ({ playerList, windowWidth }) => {

  const location = useLocation();
  const dontShowSearchOnPaths = ['/'];


  return (
    <nav className="navbar">
      <Link to="/" className="brand-logo"><img src={NavLogo} alt='Wager Wiser'/></Link>
      <div className="navbar-center">
        {!dontShowSearchOnPaths.includes(location.pathname) && <SearchBar playerList={playerList} />}
      </div>
      <div className='LoginNavContainer'>
        <LoginButtons windowWidth={windowWidth}/>
      </div>
    </nav>
  );
};

export default NavBar;